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
	"bytes"
//	"errors"
)

// TODO
// replace relative times with absolute.
// split up into resonable functions and files 
// listen on UDP , TCP
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
// make senml package with Marshal and Unmarshal names


// test with something like:  curl --data @data.json http://localhost:8000/data
// curl --data-binary @data.cbor http://localhost:8001/data


type SenMLRecord struct {
	XMLName *bool `json:"_,omitempty" xml:"senml"`
	
	BaseName string `json:"bn,omitempty"  xml:"bn,attr,omitempty"`
	BaseTime float64 `json:"bt,omitempty"  xml:"bt,attr,omitempty"`
	BaseUnit string `json:"bu,omitempty"  xml:"bu,attr,omitempty"`
	Version int `json:"ver,omitempty"  xml:"ver,attr,omitempty"`
	
	Name string `json:"n,omitempty"  xml:"n,attr,omitempty"`
	Unit string `json:"u,omitempty"  xml:"u,attr,omitempty"`
	Time float64 `json:"t,omitempty"  xml:"t,attr,omitempty"`
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
var doPrintPtr = flag.Bool("print", false, "print output to stdout")
var doExpandPtr = flag.Bool("expand", false, "expand SenML records")

var httpPort = flag.Int("http", 0, "port to list for http on")
var postUrl = flag.String("post", "", "URL to HTTP POST output to")

var doJsonPtr = flag.Bool("json", false, "output JSON formatted SenML ")
var doCborPtr = flag.Bool("cbor", false, "output CBOR formatted SenML ")
var doXmlPtr  = flag.Bool("xml",  false, "output XML formatted SenML ")
var doMpackPtr = flag.Bool("mpack", false, "output MessagePack formatted SenML ")
var doLinpPtr = flag.Bool("linp", false, "output InfluxDB LineProtcol formatted SenML ")

var doIJsonPtr = flag.Bool("ijson", false, "input JSON formatted SenML ")
var doIXmlPtr = flag.Bool("ixml", false, "input XML formatted SenML ")
var doICborPtr = flag.Bool("icbor", false, "input CBOR formatted SenML ")
var doIMpackPtr = flag.Bool("impack", false, "input MessagePack formatted SenML ")


func decodeSenMLTimed( msg []byte ) (SenML, error) {
	var senml SenML
	var err error
	
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
		
		senml,err = decodeSenML( msg  )
	}
	endParseTime1 = time.Now()
	
	if ( *doTimePtr ) {
		parseTime1 := endParseTime1.Sub( startParseTime1 )
		fmt.Printf("Parse time %v us or %v msg/s \n",
			float64( parseTime1.Nanoseconds() ) / ( float64( loops )  * 1.0e3 ),
			int64( 1.0e9 * float64( loops )  / float64( parseTime1.Nanoseconds() ) ) )
	}

	return senml,err
}


func decodeSenML( msg []byte ) (SenML, error) {
	var s SenML
	var err error
	
	s.XMLName = nil
	s.Xmlns = "urn:ietf:params:xml:ns:senml"
	
	// parse the input JSON
	if ( *doIJsonPtr ) {
		err = json.Unmarshal(msg, &s.Records )
		if err != nil {
			fmt.Printf("error parsing JSON SenML %v\n",err)
			return s,err
		}
	}
	
	// parse the input XML
	if ( *doIXmlPtr ) {
		err = xml.Unmarshal(msg, &s)
		if err != nil {
			fmt.Printf("error parsing XML SenML %v\n",err)
				return s,err
		}
	}
	
	// parse the input CBOR
	if ( *doICborPtr ) {
		var cborHandle codec.Handle = new( codec.CborHandle )
		var decoder *codec.Decoder = codec.NewDecoderBytes( msg, cborHandle )
		err = decoder.Decode( &s.Records )
		if err != nil {
			fmt.Printf("error parsing CBOR SenML %v\n",err)
			return s,err
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
			return s,err
		}
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
			return nil, err
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
			return nil, err
		}
	}

	// output a CBOR version 
	if ( *doCborPtr ) {
		var cborHandle codec.Handle = new(codec.CborHandle)
		var encoder *codec.Encoder = codec.NewEncoderBytes( &data, cborHandle)
		err = encoder.Encode( s.Records )
		if err != nil {
			fmt.Printf("error encoding CBOR SenML %v\n",err)
			return nil, err
		}
	}

	// output a MPACK version 
	if ( *doMpackPtr ) {
		var mpackHandle codec.Handle = new(codec.MsgpackHandle)
		var encoder *codec.Encoder = codec.NewEncoderBytes( &data, mpackHandle)
		err = encoder.Encode( s.Records )
		if err != nil {
			fmt.Printf("error encoding MPACK SenML %v\n",err)
			return nil, err
		}

	}

	if ( *doLinpPtr ) {
		var lines string
		for _,r := range s.Records {
			if r.Value != nil {
				lines += fmt.Sprintf( "%s", r.Name )
				if len( r.Unit ) > 0 {
					lines += fmt.Sprintf( ",u=%s", r.Unit )
				} 
				lines += fmt.Sprintf( " value=%f", *r.Value )
				lines += fmt.Sprintf( " %d\n", r.Time )
			}
		}
		data = []byte( lines )
	}
	
	if ( *doSizePtr ) {
		fmt.Printf("Output message size = %v\n", len( data ) )
	}

	return data , nil
}

func expandSenML( senml SenML) ( SenML ){
	var bname string = ""
	var btime float64 = 0.0
	var bunit string = ""
	var ver = 3
	var ret SenML

	var totalRecords int = 0
	for _,r := range senml.Records {
		if (r.Value != nil) || ( len(r.StringValue)>0) || (r.BoolValue != nil) {
			totalRecords += 1
		}
	}
	
	ret.XMLName = senml.XMLName
	ret.Xmlns = senml.Xmlns
	ret.Records = make( []SenMLRecord, totalRecords )
	var numRecords =0
	
	for _,r := range senml.Records {
		if r.BaseTime != 0 {
			btime = r.BaseTime
		}
		if r.Version != 0 {
			ver = r.Version
		}
		if len(r.BaseUnit) >0 {
			bunit = r.BaseUnit
		}
		if len(r.BaseName) >0 {
			bname = r.BaseName
		}
		r.BaseTime = 0
		r.BaseUnit = ""
		r.BaseName = ""
		r.Name = bname + r.Name
		r.Time = btime + r.Time
		if len(r.Unit) == 0 {
			r.Unit = bunit
		}
		r.Version = ver

		if  ( r.Time <= 0 )	{
			// convert to absolute time
			var now int64 = time.Now().UnixNano()
			var t float64 = float64( now ) / 1.0e9
			r.Time = t + r.Time
		}
		
		if (r.Value != nil) || ( len(r.StringValue)>0) || (r.BoolValue != nil) {
			ret.Records[numRecords] = r
			numRecords += 1
		}
	}

	return ret
}


func outputData( data []byte ) ( error ) {
	// print the output

	if *doPrintPtr {
		println( string( data ) )
	}

	if len(*postUrl) != 0  {
		println( "PostURL=<" + string(*postUrl) + ">" )
		buffer := bytes.NewBuffer( data )
		_, err := http.Post( string(*postUrl) , "image/jpeg", buffer )
		if err != nil {
			println( "Post to",  string(*postUrl) ," got error", err.Error() )
			return err
		}
	}
	
	return nil
}


func processData( dataIn []byte ) ( error ) {
	var senml SenML
	var err error

	//println( "DataIn:", dataIn )
	
	senml,err = decodeSenMLTimed( dataIn )
	if err != nil {
		println( "Decode of SenML failed" )
		return err
	}

	//println( "Senml:", senml.Records )
	if *doExpandPtr {
		senml = expandSenML( senml )
	}
	
	var dataOut []byte;
	dataOut, err = encodeSenML( senml )
	if err != nil {
		println( "Encode of SenML failed" )
		return err
	}

	//println( "DataOut:", dataOut )

	err = outputData( dataOut )
	if err != nil {
		println( "Output of SenML failed" )
		return err
	}

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

	err = processData( body )
	if err != nil {
		http.Error(w, err.Error(), 400)
	}
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


