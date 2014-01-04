package dynamo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	// Data types that DynamoDB supports.
	TypeString    = "S"
	TypeNumber    = "N"
	TypeBinary    = "B"
	TypeStringSet = "SS"
	TypeNumberSet = "NS"
	TypeBinarySet = "BS"

	TypeHashKey  = "HASH"
	TypeRangeKey = "RANGE"

	// Target endpoints.
	DynamoBaseEndpoint     = "DynamoDB_20120810."
	ListTablesEndpoint     = "ListTables"
	CreateTableEndpoint    = "CreateTable"
	DescribeTableEndpoint  = "DescribeTable"
	UpdateTableEndpoint    = "UpdateTable"
	DeleteTableEndpoint    = "DeleteTable"
	PutItemEndpoint        = "PutItem"
	GetItemEndpoint        = "GetItem"
	UpdateItemEndpoint     = "UpdateItem"
	DeleteItemEndpoint     = "DeleteItem"
	BatchGetItemEndpoint   = "BatchGetItem"
	BatchWriteItemEndpoint = "BatchWriteItem"
	QueryEndpoint          = "Query"
	ScanEndpoint           = "Scan"

	IllegalChars          = "$%^" // TODO(joy): Find out what is legal for table names and attributes.
	omitEmptyTag          = "omitempty"
	ignoreTag             = "-"
	numDigitsPrecision    = 38
	RequestSizeLimitBytes = 1000000
	minTableLength        = 3
	maxTableLength        = 255
	BatchWriteItemLimit   = 25
)

var NumberRegex *regexp.Regexp

type Client struct {
	c        *http.Client
	signer   *aws.V4Signer
	Auth     aws.Auth
	Region   aws.Region
	Endpoint string
}

type Request struct {
	req *http.Request
}

func init() {
	NumberRegex = regexp.MustCompile(`[0-9]+(\.[0-9]+(E?[-+]?[0-9]+)?)?`)
}

func NewClient(auth aws.Auth, region aws.Region) *Client {
	return &Client{
		Auth:     auth,
		Region:   region,
		Endpoint: region.DynamoDBEndpoint,
		c:        &http.Client{},
		signer:   aws.NewV4Signer(auth, "dynamodb", region),
	}
}

//  Test wheather you can set the content afterwards (you can set Content lenght, but not sure if the content is used in the signing)

func (c *Client) NewRequest(endpoint string) (*Request, error) {
	req, err := http.NewRequest("POST", c.Region.DynamoDBEndpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Date", time.Now().UTC().Format(aws.ISO8601BasicFormat))
	req.Header.Set("X-Amz-Target", DynamoBaseEndpoint+endpoint)
	return &Request{req}, nil
}

func (c *Client) NewRequestWithContent(endpoint string, data interface{}) (*Request, error) {
	req, err := c.NewRequest(endpoint)
	if err != nil {
		return nil, err
	}
	return req, req.SetContent(data)
}

func (c *Client) Do(r *Request) (*http.Response, error) {
	c.signer.Sign(r.req)
	res, err := c.c.Do(r.req)
	if err != nil {
		return res, err
	} else if res.StatusCode != http.StatusOK {
		if b, err := ioutil.ReadAll(res.Body); err != nil {
			return res, fmt.Errorf("Could not read response, status code was %d: %s", res.StatusCode, err.Error())
		} else {
			return res, fmt.Errorf("Status code %d returned: %s", res.StatusCode, string(b))
		}
	}
	return res, nil
}

func (c *Client) RawQuery(q Query) ([]AttributeSet, AttributeSet, error) {
	res := QueryResponse{}
	err := c.makeRequest(QueryEndpoint, q, &res)
	fmt.Println("Full response", res)
	return res.Items, res.LastEvaluatedKey, err
}

func (c *Client) BatchWrite(table string, items interface{}) (BatchResponse, error) {
	// TODO(joy): Check that the total payload size is less than 1MB.
	req, res := BatchWriteRequest{}, BatchResponse{}
	v := reflect.ValueOf(items)
	if k := v.Kind(); k != reflect.Array && k != reflect.Slice {
		return res, fmt.Errorf("Items must be array or slice, was %v", k)
	}
	l := v.Len()
	if l > BatchWriteItemLimit {
		return res, errors.New("Maximum of 25 item limit for batch writes exceeded")
	}
	reqItems := make([]RequestItem, l)
	for i := 0; i < v.Len(); i++ {
		item := v.Index(i).Interface()
		attr, err := MarshalAttributes(item)
		if err != nil {
			return res, err
		}
		reqItems[i].PutRequest = &PutRequest{Item: attr}
	}
	req.RequestItems = map[string][]RequestItem{table: reqItems}
	return res, c.makeRequest(BatchWriteItemEndpoint, req, &res)
}

func (r *Request) SetContentBytes(data []byte) {
	r.req.Body = ioutil.NopCloser(bytes.NewBuffer(data))
	r.req.ContentLength = int64(len(data))
}

func (r *Request) SetContentString(data string) {
	r.req.Body = ioutil.NopCloser(bytes.NewBufferString(data))
	r.req.ContentLength = int64(len(data))
}

func (r *Request) SetContent(data interface{}) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	r.req.Body = ioutil.NopCloser(bytes.NewBuffer(b))
	r.req.ContentLength = int64(len(b))
	return err
}

func (c *Client) PutItem(table string, doc interface{}) error {
	// TODO(joy): Check that the total payload size is less than 1MB, and item size is less than 64kb.
	item, err := MarshalAttributes(doc)
	if err != nil {
		return err
	}
	data := PutRequest{
		BasicRequest: BasicRequest{TableName: table},
		Item:         item,
	}
	r, err := c.NewRequestWithContent(PutItemEndpoint, data)
	if err != nil {
		return err
	}
	_, err = c.Do(r)
	return err
}

func (c *Client) UpdateItem(table string, matchDoc interface{}, updates interface{}, updateType string) error {
	key, err := MarshalAttributes(matchDoc)
	if err != nil {
		return err
	} else if len(key) > 2 {
		// TODO: Use extra attributes as expected values?
		return fmt.Errorf("Document contains %d attributes, should only contain hashkey and range key", len(key))
	}
	attr, err := MarshalAttributes(updates)
	if err != nil {
		return err
	}
	updateAttr := map[string]AttributeUpdate{}
	for a, val := range attr {
		if _, ok := key[a]; !ok {
			updateAttr[a] = AttributeUpdate{Value: val, Action: updateType}
		}
	}
	req := Update{
		TableName:        table,
		Key:              key,
		AttributeUpdates: updateAttr,
	}
	return c.makeRequest(UpdateItemEndpoint, req, &UpdateResponse{})
}

func (c *Client) CreateTableSimple(name, hashKeyName, hashKeyType, rangeKeyName, rangeKeyType string, read, write int) (TableDescription, error) {
	res := TableDescriptionWrapper{}
	if read == 0 || write == 0 {
		return res.Description, errors.New("Read/Write throughput may not be 0")
	} else if len(name) < 3 {
		return res.Description, errors.New("Table name must be at least 3 characters")
	} else if !isValidType(hashKeyType) {
		return res.Description, fmt.Errorf("%q is invalid attribute type", hashKeyType)
	} else if len(rangeKeyName) > 0 && !isValidType(rangeKeyType) {
		return res.Description, fmt.Errorf("%q is invalid attribute type", rangeKeyType)
	}
	req := TableRequest{
		TableName:             name,
		ProvisionedThroughput: Throughput{ReadUnits: read, WriteUnits: write},
		KeySchema:             []Key{{Name: hashKeyName, Type: TypeHashKey}},
		AttributeDefinitions:  []AttributeDefinition{{Name: hashKeyName, Type: hashKeyType}},
	}
	if len(rangeKeyName) > 0 {
		req.KeySchema = append(req.KeySchema, Key{Name: rangeKeyName, Type: TypeRangeKey})
		req.AttributeDefinitions = append(req.AttributeDefinitions, AttributeDefinition{Name: rangeKeyName, Type: rangeKeyType})
	}
	err := c.makeRequest(CreateTableEndpoint, req, &res)
	return res.Description, err
}

func (c *Client) DeleteTable(name string) (TableDescription, error) {
	req, res := TableRequest{TableName: name}, TableDescriptionWrapper{}
	return res.Description, c.makeRequest(DeleteTableEndpoint, req, &res)
}

func (c *Client) BatchGetRaw(table string, keys []AttributeSet, filter []string) ([]AttributeSet, []RequestItem, error) {
	req := BatchGetRequest{
		RequestItems: map[string]RequestItem{table: RequestItem{AttributesToGet: filter, Keys: keys}},
	}
	res := BatchResponse{}
	return res.Responses[table], res.UnprocessedItems[table], c.makeRequest(BatchGetItemEndpoint, req, &res)
}

func (c *Client) DoAndUnmarshal(r *Request, dst interface{}) error {
	res, err := c.Do(r)
	if err != nil {
		return err
	}
	return unmarshalResponse(res.Body, dst)
}

func (c *Client) makeRequest(endpoint string, data, dst interface{}) error {
	req, err := c.NewRequestWithContent(endpoint, data)
	if err != nil {
		return err
	} else if dst != nil {
		return c.DoAndUnmarshal(req, dst)
	}
	_, err = c.Do(req)
	return err
}

func (c *Client) ChangeThroughput(table string, read, write int) error {
	req := TableRequest{
		TableName:             table,
		ProvisionedThroughput: Throughput{ReadUnits: read, WriteUnits: write},
	}
	return c.makeRequest(UpdateTableEndpoint, req, nil)
}

// ListTables returns a limit of 100 tables.
func (c *Client) ListTables(start string, limit int) ([]string, string, error) {
	req := ListTablesRequest{
		ExclusiveStartTableName: start,
		Limit: limit,
	}
	res := ListTablesResponse{}
	return res.TableNames, res.LastEvaluatedTableName, c.makeRequest(ListTablesEndpoint, req, &res)
}

func isValidType(attrType string) bool {
	switch attrType {
	case TypeBinary, TypeNumber, TypeString, TypeBinarySet, TypeNumberSet, TypeStringSet:
		return true
	}
	return false
}

func (c *Client) DescribeTable(table string) (TableDescription, error) {
	td := TableDescriptionWrapper{}
	r, err := c.NewRequest(DescribeTableEndpoint)
	if err != nil {
		return td.Table, err
	}
	r.SetContent(BasicRequest{TableName: table})
	res, err := c.Do(r)
	if err != nil {
		return td.Table, err
	}
	err = unmarshalResponse(res.Body, &td)
	return td.Table, err
}

func unmarshalResponse(data io.Reader, dst interface{}) error {
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// By default because Dynamo doesn't allow empty attributes, so empty arrays, pointers, string, etc. values are not stored.
// Thus 'omitempty' (or the lack thereof) is only significant for pointers, maps, and structs.
func MarshalAttributes(i interface{}) (attr AttributeSet, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = fmt.Errorf("Error: %v", r)
		}
	}()
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	k := v.Kind()
	if k == reflect.Ptr {
		v = v.Elem()
		t = reflect.TypeOf(v.Interface())
	}
	k = v.Kind()
	if k != reflect.Struct {
		return nil, fmt.Errorf("Type was not struct or ptr to struct, was %v", k)
	}
	attr = AttributeSet{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := f.Name
		tag, forceType, omitempty := f.Tag.Get("dynamo"), "", false
		if len(tag) > 0 {
			if tag == ignoreTag {
				continue
			}
			tagParts := strings.Split(tag, ",")
			if len(tagParts[0]) > 0 {
				name = tagParts[0]
			}
			for j := 1; j < len(tagParts); j++ {
				switch tagParts[j] {
				case omitEmptyTag:
					omitempty = true
				case TypeNumber, TypeString, TypeBinary, TypeBinarySet, TypeNumberSet, TypeStringSet:
					forceType = tagParts[j]
				}
			}
		}
		fv := v.Field(i)
		if isEmptyValue(fv) {
			if !omitempty {
				// TODO(joy): If omitempty not specified, the attribute for false boolean and zero numeric values *should* still be set.
			}
			continue
		}
		var val AttributeVal
		// The struct tag may specify what type dynamo should store this field as. If not specified, the native type will be used.
		// TODO(joy): Check that the forced type is valid for the value given (for Number and Set types).
		if len(forceType) > 0 {
			switch forceType {
			case TypeString:
				val.S = getStringValue(fv)
			case TypeStringSet:
				val.SS = getStringArray(fv)
			case TypeNumber:
				val.N = getStringValue(fv)
			case TypeNumberSet:
				val.NS = getStringArray(fv)
			}
		} else {
			val = getAttribute(fv)
		}
		if val.IsValid() {
			if _, ok := attr[name]; ok {
				panic("Multiple attributes have same designated name")
			}
			attr[name] = val
		}
	}
	return
}

// TODO: Figure out where to use Binary types.
func getAttribute(v reflect.Value) AttributeVal {
	switch v.Kind() {
	case reflect.Array, reflect.Slice:
		vals := getStringArray(v)
		e := v.Index(0)
		switch e.Kind() {
		// Check if they're pointers to a number type, otherwise default on putting into the SS category. Thus []*int would get
		case reflect.Ptr:
			switch e.Elem().Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64:
				return AttributeVal{NS: vals}
			}
			return AttributeVal{SS: vals}
		case reflect.String, reflect.Struct, reflect.Map, reflect.Interface:
			return AttributeVal{SS: vals}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
			return AttributeVal{NS: vals}
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Float32, reflect.Float64:
		return AttributeVal{N: getStringValue(v)}
	default:
		return AttributeVal{S: getStringValue(v)}
	}
	panic("You shouldn't be here!")
}

func getStringValue(v reflect.Value) string {
	// TODO(joy): Take care of types Uintptr, Complex64, Complex128, UnsafePointer.
	switch v.Kind() {
	case reflect.Invalid:
		panic(fmt.Errorf("Invalid data type %v", v.Kind()))
	case reflect.String:
		return v.String()
	case reflect.Bool:
		if v.Bool() {
			return "1"
		}
		return "0"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(v.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(v.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(v.Float(), 'G', numDigitsPrecision, 64)
	case reflect.Struct, reflect.Map, reflect.Array, reflect.Slice:
		bytes, err := json.Marshal(v.Interface())
		if err != nil {
			panic(fmt.Errorf("Invalid json: %s", err.Error()))
		}
		return string(bytes)
		// TODO(joy): Take care of these cases. Probably will make array of strings (the JSONs).
	case reflect.Ptr, reflect.Interface:
		if v.IsNil() {
			return ""
		}
		return getStringValue(v.Elem())
	case reflect.Func, reflect.Chan:
		return ""
	}
	panic(fmt.Errorf("Invalid data type %v", v.Kind()))
}

func getStringArray(v reflect.Value) []string {
	n := v.Len()
	res := make([]string, n)
	k := 0
	for i := 0; i < n; i++ {
		res[k] = getStringValue(v.Index(i))
		if len(res[k]) > 0 {
			k++
		}
	}
	return res[:k]
}

func isEmptyValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	}
	return false
}
