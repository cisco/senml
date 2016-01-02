package main

import (
	"fmt"
	"encoding/json"
	"encoding/xml"
	"github.com/ugorji/go/codec"
	"flag"
	"os"
	"io/ioutil"
	"time"
	"net/http"
	"strconv"
)

// TODO
// replace relative times with absolute.
// split up into resonable functions and files 
// listen on HTTP , UDP , TCP
// write to HTTP POST
// write to multiple locations
// write usage doc 
// write to influxdb
// write to Kafka
// auto guese input format if not on CLI 
// support EXI ??
// write to COAP post
// listen on COAP
// read old v=1 senml and convert to v=3
// perfornance measurements and size measurements
// better CLI -if json -of xml -i tcp:1234 -o http 2345 

// test with something like:  curl --data @data.json http://localhost:8000/data


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
	StringValue string `json:"vs,omitempty"  xml:"vs,attr,omitempty"`
	BoolValue *bool `json:"vb,omitempty"  xml:"vb,attr,omitempty"`
	
	Sum *float64 `json:"s,omitempty"  xml:"sum,,attr,omitempty"`
}  


type SenML struct {
	XMLName *bool `json:"_,omitempty" xml:"sensml"`
	Xmlns string `json:"_,omitempty" xml:"xmlns,attr"`
	
	Records []SenMLRecord  ` xml:"senml"`
}

var doIndentPtr = flag.Bool("i", false, "indent output")
var doTimePtr = flag.Bool("time", false, "time parsing of input")
var doSizePtr = flag.Bool("size", false, "report size of output")

var httpPort = flag.Int("http", 0, "port to list for http on")

var doJsonPtr = flag.Bool("json", false, "output JSON formatted SENML ")
var doCborPtr = flag.Bool("cbor", false, "output CBOR formatted SENML ")
var doXmlPtr  = flag.Bool("xml",  false, "output XML formatted SENML ")
var doMpackPtr = flag.Bool("mpack", false, "output MessagePack formatted SENML ")

var doIJsonPtr = flag.Bool("ijson", false, "input JSON formatted SENML ")
var doIXmlPtr = flag.Bool("ixml", false, "input XML formatted SENML ")
var doICborPtr = flag.Bool("icbor", false, "input CBOR formatted SENML ")
var doIMpackPtr = flag.Bool("impack", false, "input MessagePack formatted SENML ")


func decodeSenML( msg []byte ) (SenML, error) {
	var s SenML
	var err error
	
	s.XMLName = nil
	s.Xmlns = "urn:ietf:params:xml:ns:senml"

	startParseTime1 := time.Now();
	endParseTime1 := time.Now();
	var loops int = 0
	if ( *doTimePtr ) {
		loops = 1000
	}
	for i := -1; i < loops; i++ {
		if i == 0 {
			startParseTime1 = time.Now();
		}
		
		// parse the input JSON
		if ( *doIJsonPtr ) {
			err = json.Unmarshal(msg, &s.Records )
			if err != nil {
				fmt.Printf("error parsing JSON SenML %v\n",err)
				os.Exit( 1 )
			}
		}
		
		// parse the input XML
		if ( *doIXmlPtr ) {
			err = xml.Unmarshal(msg, &s)
			if err != nil {
				fmt.Printf("error parsing XML SenML %v\n",err)
				os.Exit( 1 )
			}
		}
		
		// parse the input CBOR
		if ( *doICborPtr ) {
			var cborHandle codec.Handle = new( codec.CborHandle )
			var decoder *codec.Decoder = codec.NewDecoderBytes( msg, cborHandle )
			err = decoder.Decode( &s.Records )
			if err != nil {
				fmt.Printf("error parsing CBOR SenML %v\n",err)
				os.Exit( 1 )
			}
		}
		
		// parse the input MPACK
		// spec for MessagePack is at https://github.com/msgpack/msgpack/
		if ( *doIMpackPtr ) {
			var mpackHandle codec.Handle = new( codec.MsgpackHandle )
			var decoder *codec.Decoder = codec.NewDecoderBytes( msg, mpackHandle )
			err = decoder.Decode( &s.Records )
			if err != nil {
				fmt.Printf("error parsing MPACK SenML %v\n",err)
				os.Exit( 1 )
			}
		}
		
	}
	endParseTime1 = time.Now()

	if ( *doTimePtr ) {
		parseTime1 := endParseTime1.Sub( startParseTime1 )
		fmt.Printf("Parse time %v us or %v msg/s \n",
			float64( parseTime1.Nanoseconds() ) / ( float64( loops )  * 1.0e3 ),
			int64( 1.0e9 * float64( loops )  / float64( parseTime1.Nanoseconds() ) ) )
	}

	return s , nil 
}

func encodeSenML( s SenML ) ( []byte, error ) {
	var data []byte
	var err error
		
	// ouput JSON version 
	if ( *doJsonPtr ) {
		if ( *doIndentPtr ) {
			data, err = json.MarshalIndent( s.Records, "", "  " )
		} else {
			data, err = json.Marshal( s.Records )
		}
		if err != nil {
			fmt.Printf("error encoding JSON SenML %v\n",err)
			os.Exit( 1 )
		}
	}

	// output a XML version 
	if ( *doXmlPtr ) {
		if ( *doIndentPtr ) {
			data, err = xml.MarshalIndent( s, "", "  " )
		} else {
			data, err = xml.Marshal( s )
		}
		if err != nil {
			fmt.Printf("error encoding XML SenML %v\n",err);	
		}
	}

	// output a CBOR version 
	if ( *doCborPtr ) {
		var cborHandle codec.Handle = new(codec.CborHandle)
		var encoder *codec.Encoder = codec.NewEncoderBytes( &data, cborHandle)
		err = encoder.Encode( s.Records )
		if err != nil {
			fmt.Printf("error encoding CBOR SenML %v\n",err)
			os.Exit( 1 )
		}
	}

	// output a MPACK version 
	if ( *doMpackPtr ) {
		var mpackHandle codec.Handle = new(codec.MsgpackHandle)
		var encoder *codec.Encoder = codec.NewEncoderBytes( &data, mpackHandle)
		err = encoder.Encode( s.Records )
		if err != nil {
			fmt.Printf("error encoding MPACK SenML %v\n",err)
			os.Exit( 1 )
		}
	}

	if ( *doSizePtr ) {
		fmt.Printf("Output message size = %v\n", len( data ) )
	}

	return data , nil
}


func processData( dataIn []byte ) ( error ) {
	var senml SenML
	var err error
	
	senml,err = decodeSenML( dataIn )
	if err != nil {
		return err
	}
	
	var dataOut []byte;
	dataOut, err = encodeSenML( senml )
	if err != nil {
		return err
	}
	// print the output
	println( string(dataOut) )

	return nil
}


func httpReqHandler(w http.ResponseWriter, r *http.Request) {
	//println( "HTTP request to ",  r.URL.Path )
	//println( "Method: ",  r.Method )

	defer r.Body.Close()
	body, err := ioutil.ReadAll( r.Body)
	if err != nil {
		panic( "Problem reading HTTP body" )
	}
	//println( "Body: ",  string(body) )

	err = processData( body )
}


func main() {
	flag.Parse()

	if  *httpPort != 0 {
		http.HandleFunc("/", httpReqHandler)
		http.ListenAndServe(":"+strconv.Itoa(*httpPort), nil)
	} else {
		// load the input  
		msg, err := ioutil.ReadFile( flag.Arg(0) )
		if err != nil {
			fmt.Printf("error reading SenML file %v\n",err)
			os.Exit( 1 )
		}	
		//fmt.Print(string(msg))
		
		err = processData( msg )
	}
}


