package dynamo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/crowdmob/goamz/aws"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"reflect"
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

	// Query condition operators.
	ConditionEqual              = "EQ"
	ConditionNotEqual           = "NE"
	ConditionLessThanOrEqual    = "LE"
	ConditionLessThan           = "LT"
	ConditionGreaterThanOrEqual = "GE"
	ConditionGreaterThan        = "GT"
	ConditionBeginsWith         = "BEGINS_WITH"
	ConditionBetween            = "BETWEEN"

	// Scan condition operators.
	ConditionContains           = "CONTAINS"
	ConditionNotContains        = "NOT_CONTAINS"
	ConditionAttributeExists    = "NOT_NULL"
	ConditionAttributeNotExists = "NULL"
	ConditionIn                 = "IN"

	// Query selection operators.
	SelectAll       = "ALL_ATTRIBUTES"
	SelectProjected = "ALL_PROJECTED_ATTRIBUTES"
	SelectSpecific  = "SPECIFIC_ATTRIBUTES"
	SelectCount     = "COUNT"

	// Commonly encountered errors.
	ProvisionedThroughputExceededException = "ProvisionedThroughputExceededException"
	ResourceNotFoundExcpetion              = "ResourceNotFoundException"

	// Target endpoints.
	DynamoBaseEndpoint     = "DynamoDB_20120810."
	ListTablesEndpoint     = "ListTables"
	CreateTableEndpoint    = "CreateTable"
	DescribeTableEndpoint  = "DescribeTable"
	UpdateTableEndpoint    = "UpdateTable"
	DeleteTableEndpoint    = "DeleteTable"
	PutItemEndpoint        = "PutItem"
	GetItemEndpoint        = "GetItem"
	DeleteItemEndpoint     = "DeleteItem"
	BatchGetItemEndpoint   = "BatchGetItem"
	BatchWriteItemEndpoint = "BatchWriteItem"
	QueryEndpoint          = "Query"
	ScanEndpoint           = "Scan"

	IllegalChars = "$%^"
	OmitEmptyTag = "omitempty"

	numHashKeys = int64(500)

	largePrime = int64(1125899839733759)

	numDigitsPrecision = 37
)

// This gives numbers in range 0-512! -- need to scale to 500
func HashKey1(id int64) int {
	A := int64(math.Floor(0.5 * (math.Sqrt(5.0) - 1.0)))
	id = id * A
	return int(id >> 54)
}

func KnuthHashKey(id int64) int {
	return int((id * (id + 3)) % numHashKeys)
}

func HashKey2(id int64) int {
	return int((id * largePrime) % numHashKeys)
}

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

func (c *Client) DoAndUnmarshal(r *Request, dst interface{}) error {
	res, err := c.Do(r)
	if err != nil {
		return err
	}
	return unmarshalResponse(res.Body, dst)
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
	fmt.Printf("Setting content: %s\n", string(b))
	r.req.Body = ioutil.NopCloser(bytes.NewBuffer(b))
	r.req.ContentLength = int64(len(b))
	return err
}

func (c *Client) PutItem(table string, doc interface{}) error {
	r, err := c.NewRequest(PutItemEndpoint)
	if err != nil {
		return err
	}
	item, err := MarshalAttributes(doc)
	if err != nil {
		return err
	}
	data := PutRequest{
		BasicRequest: BasicRequest{TableName: table},
		Item:         item,
	}
	fmt.Println("ATTR", data.Item)
	r.SetContent(data)
	res, err := c.Do(r)
	fmt.Println("err", err)
	fmt.Println("res", res)
	return err
}

func (c *Client) CreateTableSimple(name, hashKeyName, hashKeyType, rangeKeyName, rangeKeyType string, read, write int) (TableDescription, error) {
	td := TableDescription{}
	if !isValidType(hashKeyType) {
		return td, fmt.Errorf("%q is invalid attribute type", hashKeyType)
	} else if len(rangeKeyName) > 0 && !isValidType(rangeKeyType) {
		return td, fmt.Errorf("%q is invalid attribute type", rangeKeyType)
	}
	reqData := CreateTableRequest{
		BasicRequest:          BasicRequest{TableName: name},
		ProvisionedThroughput: Throughput{ReadUnits: read, WriteUnits: write},
		KeySchema:             []Key{{Name: hashKeyName, Type: "HASH"}},
		AttributeDefinitions:  []AttributeDefinition{{Name: hashKeyName, Type: hashKeyType}},
	}
	if len(rangeKeyName) > 0 {
		reqData.KeySchema = append(reqData.KeySchema, Key{Name: rangeKeyName, Type: rangeKeyType})
		reqData.AttributeDefinitions = append(reqData.AttributeDefinitions, AttributeDefinition{Name: rangeKeyName, Type: "RANGE"})
	}
	req, err := c.NewRequest(CreateTableEndpoint)
	if err != nil {
		return td, err
	}
	req.SetContent(reqData)
	res := TableDescriptionWrapper{}
	err = c.DoAndUnmarshal(req, &res)
	fmt.Println("err", err)
	fmt.Println("res", res)
	return td, err
}

func (c *Client) ListTables(start string, limit int) ([]string, string, error) {
	req, err := c.NewRequest(ListTablesEndpoint)
	if err != nil {
		return nil, "", err
	}
	res, err := c.Do(req)
	return nil, "", err
}

func isValidType(attrType string) bool {
	switch attrType {
	case TypeBinary, TypeNumber, TypeString, TypeBinarySet, TypeNumberSet, TypeStringSet:
		return true
	}
	return false
}

func (c *Client) DescribeTable(table string) (TableDescription, error) {
	td := TableDescription{}
	r, err := c.NewRequest(DescribeTableEndpoint)
	if err != nil {
		return td, err
	}
	r.SetContent(BasicRequest{TableName: table})
	res, err := c.Do(r)
	if err != nil {
		return td, err
	}
	err = unmarshalResponse(res.Body, &td)
	return td, err
}

func unmarshalResponse(data io.Reader, dst interface{}) error {
	b, err := ioutil.ReadAll(data)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, dst)
}

// ISSUE: Basic alarms not turned on when creating table normally.

// Errors I should return: if there is a dynamo tag but cannot be marshaled (i.e. a func). Investigate what json does.
func MarshalAttributes(i interface{}) (attr AttributeSet, err error) {
	defer func() {
		if r := recover(); r != nil {
			if _, ok := r.(runtime.Error); ok {
				panic(r)
			}
			err = r.(error)
		}
	}()
	t, v := reflect.TypeOf(i), reflect.ValueOf(i)
	attr = AttributeSet{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		name := f.Name
		tag := f.Tag.Get("dynamo")
		if len(tag) > 0 {
			tagParts := strings.Split(tag, ",")
			if tagParts[0] == "-" {
				continue
			} else if len(tagParts[0]) > 0 {
				name = tagParts[0]
			}
		}
		fv := v.Field(i)
		attr[name] = getAttribute(fv)
	}
	return
}

// What happens when you have slice of interface{}. What happens when detecting the type of an member?

func getAttribute(v reflect.Value) AttributeVal {
	switch v.Kind() {
	case reflect.String:
		return AttributeVal{S: v.String()}
	case reflect.Array, reflect.Slice:
		vals := getStringArray(v)
		e := v.Index(0)
		switch e.Kind() {
		case reflect.String:
			return AttributeVal{SS: vals}
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
			return AttributeVal{NS: vals}
		}
	case reflect.Bool:
		if v.Bool() {
			return AttributeVal{N: "1"}
		} else {
			return AttributeVal{N: "0"}
		}
	case reflect.Map, reflect.Interface, reflect.Struct, reflect.Ptr:
		// TODO(joy): Treat reflect.Ptr specially as it may be a pointer to an array.
		bytes, err := json.Marshal(v.Interface())
		if err != nil {
			panic(fmt.Sprintf("Invalid json: %s", err.Error()))
		}
		return AttributeVal{S: string(bytes)}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return AttributeVal{N: strconv.FormatInt(v.Int(), 10)}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return AttributeVal{N: strconv.FormatUint(v.Uint(), 10)}
	case reflect.Float32, reflect.Float64:
		return AttributeVal{N: strconv.FormatFloat(v.Float(), 'f', numDigitsPrecision, 64)}
	}
	panic("Invalid data type")
}

func getStringArray(v reflect.Value) []string {
	n := v.Len()
	res := make([]string, n)
	for i := 0; i < n; i++ {
		e := v.Index(i)
		switch e.Kind() {
		case reflect.String:
			res[i] = e.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			res[i] = strconv.FormatInt(e.Int(), 10)
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			res[i] = strconv.FormatUint(e.Uint(), 10)
		case reflect.Float32, reflect.Float64:
			res[i] = strconv.FormatFloat(e.Float(), 'f', numDigitsPrecision, 64) // Change to 32 for flaot32?
		}
	}
	return res
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

// func UnmarshalAttributes(attr AttributeSet, dst interface{}) error {
// 	dv := reflect.ValueOf(dst)
// 	if dv.Kind() != reflect.Ptr {
// 		return fmt.Errorf("result argument must be a slice address")
// 	}
// }

// type decodeState struct {
// 	data       []byte
// 	off        int // read offset in data
// 	scan       scanner
// 	nextscan   scanner // for calls to nextValue
// 	savedError error
// 	tempstr    string // scratch space to avoid some allocations
// 	useNumber  bool
// }

// func unmarshalAttribute(AttributeVal, reflect.Type) {
// 	defer func() {
// 		if r := recover(); r != nil {
// 			if _, ok := r.(runtime.Error); ok {
// 				panic(r)
// 			}
// 			err = r.(error)
// 		}
// 	}()

// 	rv := reflect.ValueOf(v)
// 	if rv.Kind() != reflect.Ptr || rv.IsNil() {
// 		return &InvalidUnmarshalError{reflect.TypeOf(v)}
// 	}

// 	d.scan.reset()
// 	d.value(rv)
// 	return d.savedError
// }
