package dynamo

type ListTablesRequest struct {
	ExclusiveStartTableName string `json:"ExclusiveStartTableName,omitempty"`
	Limit                   int    `json:"Limit,omitempty"`
}

type BatchRequest struct {
	ReturnConsumedCapacity      string                   `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string                   `json:"ReturnItemCollectionMetrics,omitempty"`
	RequestItems                map[string][]RequestItem `json:"RequestItems"`
}

// Test if leaving ConsistentTead out for batchGet is OK.
type RequestItem struct {
	PutRequest        PutRequest     `json:"PutRequest,omitempty"`
	DeleteRequest     DeleteRequest  `json:"DeleteRequest,omitempty"`
	AttributesToGet   []string       `json:"AttributesToGet,omitempty"`
	ConsistentRead    bool           `json:"ConsistentRead,omitempty"`
	Keys              []AttributeSet `json:"Keys,omitempty"`
	ExclusiveStartKey AttributeSet   `json:"ExclusiveStartKey,omitempty"`
	IndexName         string         `json:"IndexName,omitempty"`
	//KeyConditions
	Limit int `json:"Limit,omitempty"`
}

type DeleteRequest struct {
}

type BasicRequest struct {
	ReturnConsumedCapacity      string `json:"ReturnConsumedCapacity,omitempty"`
	ReturnItemCollectionMetrics string `json:"ReturnItemCollectionMetrics,omitempty"`
	ReturnValues                string `json:"ReturnValues,omitempty"`
	TableName                   string `json:"TableName,omitempty"`
}

type CreateTableRequest struct {
	BasicRequest
	AttributeDefinitions  []AttributeDefinition
	KeySchema             []Key
	LocalSecondaryIndexes []SecondaryIndex
	ProvisionedThroughput Throughput
}

type ListTablesResponse struct {
	LastEvaluatedTableName string
	TableNames             []string
}

type TableDescriptionWrapper struct {
	Table TableDescription `json:"TableDescription"`
}

type AttributeDefinition struct {
	Name string `json:"AttributeName"`
	Type string `json:"AttributeType"`
}

type Key struct {
	Name string `json:"AttributeName"`
	Type string `json:"KeyType"`
}

type SecondaryIndex struct {
	IndexName  string
	KeySchema  []Key
	Projection IndexProjection
}

type IndexProjection struct {
	NonKeyAttributes []string
	ProjectionType   string
}

type TableDescription struct {
	AttributeDefinitions  []AttributeDefinition `json:"AttributeDefinitions"`
	CreationDateTime      int64                 `json:"CreationDateTime"`
	ItemCount             int                   `json:"ItemCount"`
	KeySchema             []Key                 `json:"KeySchema"`
	LocalSecondaryIndexes []SecondaryIndex      `json:"LocalSecondayIndexes"`
	ProvisionedThroughput Throughput            `json:"ProvisionedThroughput"`
	TableName             string                `json:"TableName"`
	TableSizeBytes        int64                 `json:"TableSizeBytes"`
	TableStatus           string                `json:"TableStatus"`
}

type Error struct {
	StatusCode int
	Type       string `json:"__type"`
	Message    string `json:"message"`
}

type AttributeSet map[string]AttributeVal

// Test if having an empty object is bad

type PutRequest struct {
	BasicRequest
	Item AttributeSet `json:"Item"`
}

// Test what happens when you make UpdateItem/PutItem Request with multiple things

type AttributeVal struct {
	S  string   `json:"S,omitempty"`
	SS []string `json:"SS,omitempty"`
	N  string   `json:"N,omitempty"`
	NS []string `json:"NS,omitempty"`
	B  string   `json:"B,omitempty"`
	BS []string `json:"BS,omitempty"`
}

// Test what happens when one attribute has multiple values.

type TableUpdate struct {
	TableName             string     `json:"TableName"`
	ProvisionedThroughput Throughput `json:"ProvisionedThroughput"`
}

type Throughput struct {
	ReadUnits              int   `json:"ReadCapacityUnits"`
	WriteUnits             int   `json:"WriteCapacityUnits"`
	LastDecreaseDateTime   int64 `json:"LastDecreaseDateTime,omitempty"`
	LastIncreaseDateTime   int64 `json:"LastIncreaseDateTime,omitempty"`
	NumberOfDecreasesToday int   `json:"NumberOfDecreasesToday,omitempty"`
}
