package main

import (
	"fmt"
	"encoding/json"
	"flag"
	"os"
	"io/ioutil"
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

	doIndentPtr := flag.Bool("i", false, "indent output")
	flag.Parse()

	// load the input JSON 
	msg, err := ioutil.ReadFile( flag.Arg(0) )
	if err != nil {
		fmt.Printf("error reading JSON XML %v\n",err)
		os.Exit( 1 )
	}	
    //fmt.Print(string(msg))

	// parse the input JSON 
	var s SenMLStream
	err = json.Unmarshal(msg, &s)
	if err != nil {
		fmt.Printf("error parsing JSON XML %v\n",err)
		os.Exit( 1 )
	}

	var d []byte;
	if ( *doIndentPtr ) {
		d,err = json.MarshalIndent( s, "", "  " )
	} else {
		d,err = json.Marshal( s )
	}
	if err != nil {
		fmt.Printf("error encoding json %v\n",err)
		os.Exit( 1 )
	}
	fmt.Printf("%s\n", d)
}
