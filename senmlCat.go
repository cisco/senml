package main

import (
	"fmt"
	"encoding/json"
	"encoding/xml"
	"flag"
	"os"
	"io/ioutil"
)

type SenMLRecord struct {
	XMLName *bool `json:"_,omitempty" xml:"senml"`
	BaseName string `json:"bn,omitempty"  xml:"bn,attr,omitempty"`
	BaseTime int `json:"bt,omitempty"  xml:"bt,attr,omitempty"`
	BaseUnit string `json:"bu,omitempty"  xml:"bu,attr,omitempty"`
	Version int `json:"ver,omitempty"  xml:"ver,attr,omitempty"`
	Name string `json:"n,omitempty"  xml:"n,attr,omitempty"`
	Unit string `json:"u,omitempty"  xml:"u,attr,omitempty"`
	Time int `json:"t,omitempty"  xml:"t,attr,omitempty"`
	UpdateTime int `json:"ut,omitempty"  xml:"ut,attr,omitempty"`

	Value *float64 `json:"v,omitempty"  xml:"v,attr,omitempty"`
	StringValue string `json:"sv,omitempty"  xml:"sv,attr,omitempty"`
	BoolValue *bool `json:"bv,omitempty"  xml:"bv,attr,omitempty"`
	
	Sum *float64 `json:"s,omitempty"  xml:"sum,,attr,omitempty"`
}  

type SenMLStream []SenMLRecord;

type SenML struct {
	XMLName *bool `json:"_,omitempty" xml:"senmls"`
	Xmlns string `json:"_,omitempty" xml:"xmlns,attr"`
	SenMLStream
}


func main() {
	var err error;

	doIndentPtr := flag.Bool("i", false, "indent output")
	doJSONPtr := flag.Bool("json", false, "output JSON formatted SENML ")
	doXMLPtr := flag.Bool("xml", false, "output XML formatted SENML ")
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

	// ouput JSON version 
	if ( *doJSONPtr ) {
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

	// out put a XML version 
	if ( *doXMLPtr ) {
		var d []byte;
		var sxml SenML = SenML{ nil, "urn:ietf:params:xml:ns:senml", s }
		
		if ( *doIndentPtr ) {
			d,err = xml.MarshalIndent( sxml, "", "  " )
		} else {
			d,err = xml.Marshal( sxml )
		}
		if err != nil {
			fmt.Printf("error encoding xml %v\n",err);	
		}
		fmt.Printf("%s\n", d)
	}
	
}
