package main

import (
	"fmt"
	"encoding/json"
	"os"
)

type SenMLRecord struct {
	BaseName string `json:"bn,omitempty"  `
	BaseTime int `json:"bt,omitempty" `
	BaseUnit string `json:"bu,omitempty" `
	Version int `json:"ver,omitempty" `
	Name string `json:"n,omitempty" `
	Unit string `json:"u,omitempty" `
	Time int `json:"t,omitempty" `
	UpdateTime int `json:"ut,omitempty" `

	Value *float64 `json:"v,omitempty" `
	StringValue string `json:"sv,omitempty" `
	BoolValue *bool `json:"bv,omitempty" `
	
	Sum *float64 `json:"s,omitempty" `
}  

type SenMLStream []SenMLRecord;

type SenML struct {
	SenMLStream
}


func main() {
	var err error;

	// load the input JSON 
	msg := []byte( `[
             {"bn":"urn:dev:mac:0024befffe804ff1/","bt":1276020076,"bu":"A","ver":2},
             {"n":"voltage","u":"V","v":0},{"n":"current","t":-4,"v":1.3,"sv":"nice"},
             {"n":"current","t":-3,"v":1,"s":33.44},{"n":"current","v":1.7,"bv":true}]` )
    fmt.Print(string(msg))

	// parse the input JSON 
	var s SenMLStream
	err = json.Unmarshal(msg, &s)
	if err != nil {
		fmt.Printf("error parsing JSON XML %v\n",err)
		os.Exit( 1 )
	}

	var d []byte;
	d,err = json.MarshalIndent( s, "", "  " )
	//d,err = json.Marshal( s )
	if err != nil {
		fmt.Printf("error encoding json %v\n",err)
		os.Exit( 1 )
	}

	fmt.Printf("%s\n", d)
}
