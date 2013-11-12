package dynamo

import (
	"github.com/crowdmob/goamz/aws"
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
)

type Server struct { // Rename to Client? or database?
	Auth     aws.Auth
	Region   aws.Region
	Endpoint string
	Client   *http.Client // We could just use DefaultClient so maybe this really isn't needed, as we're not changing the Redirect policy or Cookie jar.
}

func NewServer(auth aws.Auth, region aws.Region) *Server {
	return &Server{Auth: auth, Region: region, Endpoint: region.DynamoDBEndpoint}
}

type Table struct {
	Name string
}

type TableRequest struct {
	TableName string               `json:"TableName"`
	Item      map[string]attribute `json:"Item"`
}

type Attribute struct {
	S  string   `json:"S,omitempty"`
	SS []string `json:"SS,omitempty"`
	N  string   `json:"N,omitempty"`
	NS []string `json:"NS,omitempty"`
	B  string   `json:"B,omitempty"`
	BS []string `json:"BS,omitempty"`
}

type TableUpdate struct {
	TableName             string                `json:"TableName"`
	ProvisionedThroughput ProvisionedThroughput `json:"ProvisionedThroughput"`
}

type ProvisionedThroughput struct {
	ReadUnits  int `json:"ReadCapacityUnits"`
	WriteUnits int `json:"WriteCapacityUnits"`
}
