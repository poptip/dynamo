package dynamo

const (
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

	UpdateTypePut    = "PUT"
	UpdateTypeDelete = "DELETE"
	UpdateTypeAdd    = "ADD"

	ReturnNone       = "NONE"
	ReturnAllOld     = "ALL_OlD"
	ReturnUpdatedOld = "UPDATED_OLD"
	ReturnAllNew     = "ALL_NEW"
	ReturnUpdateNew  = "UPDATED_NEW"

	// Return consumed capacity params.
	ConsumedTotal   = "TOTAL"
	ConsumedNone    = "NONE"
	ConsumedIndexes = "INDEXES"

	// Commonly encountered errors.
	ProvisionedThroughputExceededException = "ProvisionedThroughputExceededException"
	ResourceNotFoundExcpetion              = "ResourceNotFoundException"
)

// Table-level operations.
type ListTablesRequest struct {
	ExclusiveStartTableName string `json:",omitempty"`
	Limit                   int    `json:",omitempty"`
}

type ListTablesResponse struct {
	LastEvaluatedTableName string
	TableNames             []string
}

type TableDescriptionWrapper struct {
	Description TableDescription `json:"TableDescription"`
	Table       TableDescription `json:"Table"`
}

type TableDescription struct {
	AttributeDefinitions  []AttributeDefinition
	CreationDateTime      float64 // Expressed in scientific notation, i.e. 1.3E9, in unix seconds.
	ItemCount             int
	KeySchema             []Key
	LocalSecondaryIndexes []SecondaryIndex
	ProvisionedThroughput Throughput
	TableName             string
	TableSizeBytes        int64
	TableStatus           string
}

type TableRequest struct {
	TableName             string
	AttributeDefinitions  []AttributeDefinition `json:",omitempty"`
	KeySchema             []Key                 `json:",omitempty"`
	LocalSecondaryIndexes []SecondaryIndex      `json:",omitempty"`
	ProvisionedThroughput Throughput
}

// Table attributes.

type SecondaryIndex struct {
	IndexName  string
	KeySchema  []Key
	Projection IndexProjection
}

type IndexProjection struct {
	NonKeyAttributes []string
	ProjectionType   string
}

type Throughput struct {
	ReadUnits              int     `json:"ReadCapacityUnits"`
	WriteUnits             int     `json:"WriteCapacityUnits"`
	LastDecreaseDateTime   float64 `json:",omitempty"`
	LastIncreaseDateTime   float64 `json:",omitempty"`
	NumberOfDecreasesToday int     `json:",omitempty"`
}

// Item attributes.

type AttributeDefinition struct {
	Name string `json:"AttributeName"`
	Type string `json:"AttributeType"`
}

type Key struct {
	Name string `json:"AttributeName"`
	Type string `json:"KeyType"`
}

type AttributeSet map[string]AttributeVal

type AttributeVal struct {
	S  string   `json:"S,omitempty"`
	SS []string `json:"SS,omitempty"`
	N  string   `json:"N,omitempty"`
	NS []string `json:"NS,omitempty"`
	B  string   `json:"B,omitempty"`
	BS []string `json:"BS,omitempty"`
}

func (val AttributeVal) IsValid() bool {
	nonEmpties := 0
	if len(val.S) > 0 {
		nonEmpties++
	}
	if len(val.SS) > 0 {
		nonEmpties++
	}
	if len(val.N) > 0 {
		nonEmpties++
	}
	if len(val.NS) > 0 {
		nonEmpties++
	}
	if len(val.B) > 0 {
		nonEmpties++
	}
	if len(val.BS) > 0 {
		nonEmpties++
	}
	return nonEmpties == 1
}

// CRUD operations.

type BasicRequest struct {
	ReturnConsumedCapacity      string `json:",omitempty"`
	ReturnItemCollectionMetrics string `json:",omitempty"`
	ReturnValues                string `json:",omitempty"`
	TableName                   string
}

type PutRequest struct {
	BasicRequest
	Item AttributeSet `json:"Item"`
}

type Query struct {
	TableName              string
	AttributesToGet        []string `json:",omitempty"`
	ConsistentRead         bool
	Select                 string
	ScanIndexForward       bool         `json:",omitempty"`
	ReturnConsumedCapacity string       `json:",omitempty"`
	ExclusiveStartKey      AttributeSet `json:",omitempty"`
	IndexName              string       `json:",omitempty"`
	KeyConditions          map[string]Condition
	Limit                  int `json:",omitempty"`
}

type Update struct {
	TableName                   string
	AttributeUpdates            map[string]AttributeUpdate
	Expected                    map[string]ExpectedValue `json:",omitempty"`
	Key                         AttributeSet
	ReturnConsumedCapacity      string `json:",omitempty"`
	ReturnItemCollectionMetrics string `json:",omitempty"`
	ReturnValues                string `json:",omitempty"`
}

type AttributeUpdate struct {
	Action string
	Value  AttributeVal
}

type ExpectedValue struct {
	Exists bool
	Value  AttributeVal
}

type UpdateResponse struct {
	Attributes       AttributeSet
	ConsumedCapacity []ConsumedStats
}

type ScanRequest struct {
	TableName              string
	Select                 string
	AttributesToGet        []string     `json:",omitempty"`
	ExclusiveStartKey      AttributeSet `json:",omitempty"`
	Limit                  int          `json:",omitempty"`
	ReturnConsumedCapacity string       `json:",omitempty"`
	ScanFilter             map[string]Condition
	Segment                int
	TotalSegments          int
}

type Condition struct {
	AttributeValueList []AttributeVal
	ComparisonOperator string
}

type Scan struct {
	TableName              string
	AttributesToGet        []string `json:",omitempty"`
	ConsistentRead         bool
	Select                 string
	ScanIndexForward       bool         // true = ascending, the default if not specified.
	ReturnConsumedCapacity string       `json:",omitempty"`
	ExclusiveStartKey      AttributeSet `json:",omitempty"`
	IndexName              string       `json:",omitempty"`
	KeyConditions          map[string]Condition
	Limit                  int `json:",omitempty"`
}

// Batch operations.

type BatchWriteRequest struct {
	ReturnConsumedCapacity      string                   `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                   `json:"ReturnItemCollectionMetrics,omitempty"`
	RequestItems                map[string][]RequestItem `json:"RequestItems,omitempty"`
}

type BatchGetRequest struct {
	ReturnConsumedCapacity      string                 `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                 `json:"ReturnItemCollectionMetrics,omitempty"`
	RequestItems                map[string]RequestItem `json:"RequestItems,omitempty"`
}

type ConsumedStats struct {
	CapacityUnits int
	TableName     string // TODO: Implement TableName restrictions.
}

type QueryResponse struct {
	ConsumedCapacity ConsumedStats
	Count            int
	Items            []AttributeSet
	LastEvaluatedKey AttributeSet
}

type BatchResponse struct {
	ConsumedCapacity []ConsumedStats
	Responses        map[string][]AttributeSet
	UnprocessedItems map[string][]RequestItem
}

type RequestItem struct {
	// Have to make these pointers so that omitempty will work. Could consider splitting up this struct into Get vs. Put.
	PutRequest        *PutRequest    `json:",omitempty"`
	DeleteRequest     *DeleteRequest `json:",omitempty"`
	AttributesToGet   []string       `json:",omitempty"`
	ConsistentRead    bool           `json:",omitempty"`
	Keys              []AttributeSet `json:",omitempty"`
	ExclusiveStartKey AttributeSet   `json:",omitempty"`
	IndexName         string         `json:",omitempty"`
	Limit             int            `json:",omitempty"`
}

type DeleteRequest struct {
	Key AttributeSet `json:",omitempty"`
}

type Error struct {
	StatusCode int
	Type       string `json:"__type"`
	Message    string `json:"message"`
}
