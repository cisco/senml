package main

import (
	"github.com/cisco/senml" // TODO 
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
)

// TODO
// replace relative times with absolute.
// split up into resonable functions and files 
// listen on UDP , TCP
// write to multiple locations
// write usage doc 
// write to influxdb
// auto guese input format if not on CLI 
// support EXI ??
// write to COAP post
// listen on COAP
// read old v=1 senml and convert to v=3
// perfornance measurements and size measurements
// better CLI -if json -of xml -i tcp:1234 -o http 2345 
// make senml package with Marshal and Unmarshal names
// if loose kafka connection, reopen it (or perhaps 1 connection per transaction)
// option to print json object (not array, one per line) to kafka

// define a CSV format for senml 

// test http with something like:  curl --data @data.json http://localhost:8000/data
// curl --data-binary @dagta.cbor http://localhost:8001/data

var doIndentPtr = flag.Bool("i", false, "indent output")
var doPrintPtr = flag.Bool("print", false, "print output to stdout")
var doExpandPtr = flag.Bool("expand", false, "expand SenML records")
var postUrl = flag.String("post", "", "URL to HTTP POST output to")
var topic = flag.String("topic", "senml", "Apache Kafka topic or InfluxDB series name ")

var doJsonPtr = flag.Bool("json", false, "output JSON formatted SenML ")
var doCborPtr = flag.Bool("cbor", false, "output CBOR formatted SenML ")
var doXmlPtr  = flag.Bool("xml",  false, "output XML formatted SenML ")
var doCsvPtr  = flag.Bool("csv",  false, "output CSV formatted SenML ")
var doMpackPtr = flag.Bool("mpack", false, "output MessagePack formatted SenML ")
var doLinpPtr = flag.Bool("linp", false, "output InfluxDB LineProtcol formatted SenML ")

var doIJsonStreamPtr = flag.Bool("ijsons", false, "input JSON formatted SenML stream")
var doIJsonLinePtr = flag.Bool("ijsonl", false, "input JSON formatted SenML lines")
var doIXmlPtr = flag.Bool("ixml", false, "input XML formatted SenML ")
var doICborPtr = flag.Bool("icbor", false, "input CBOR formatted SenML ")
var doIMpackPtr = flag.Bool("impack", false, "input MessagePack formatted SenML ")




func decodeSenMLTimed( msg []byte ) ( gosenml.SenML, error) {
	var senml gosenml.SenML
	var err error
	
	var format gosenml.Format = gosenml.JSON
	switch {
	case *doIJsonStreamPtr: format = gosenml.JSON
	case *doIJsonLinePtr: format = gosenml.JSON
	case *doICborPtr: format = gosenml.CBOR
	case *doIXmlPtr: format = gosenml.XML
	case *doIMpackPtr: format = gosenml.MPACK
	}
	
	senml,err = gosenml.DecodeSenML( msg  ,format ) // TODO 
	
	return senml,err
}


func outputData( data []byte ) ( error ) {
	if *doPrintPtr {
		fmt.Print( string( data ) )
	}
	
	if len(*postUrl) != 0  {
		fmt.Println( "PostURL=<" + string(*postUrl) + ">" )
		buffer := bytes.NewBuffer( data )
		_, err := http.Post( string(*postUrl) , "application/senml+json", buffer )
		if err != nil {
			fmt.Println( "Post to",  string(*postUrl) ," got error", err.Error() )
			return err
		}
	}
	
	return nil
}


func processData( dataIn []byte ) ( error ) {
	var senml gosenml.SenML
	var err error
	
	senml,err = decodeSenMLTimed( dataIn )
	if err != nil {
		fmt.Println( "Decode of SenML failed" )
		return err
	}

	//fmt.Println( "Senml:", gosenml.Records )
	if *doExpandPtr {
		senml = gosenml.ExpandSenML( senml )
	}

	var dataOut []byte;
	options := gosenml.OutputOptions{}
	if *doIndentPtr {
		options.PrettyPrint = *doIndentPtr
	}
	options.Topic = string( *topic )
	var format gosenml.Format = gosenml.JSON
	switch {
	case *doJsonPtr: format = gosenml.JSON
	case *doCborPtr: format = gosenml.CBOR
	case *doXmlPtr: format = gosenml.XML
	case *doCsvPtr: format = gosenml.CSV
	case *doMpackPtr: format = gosenml.MPACK
	case *doLinpPtr: format = gosenml.LINEP
	}
	dataOut, err = gosenml.EncodeSenML( senml,format, options )
	if err != nil {
		fmt.Println( "Encode of SenML failed" )
		return err
	}

	err = outputData( dataOut )
	if err != nil {
		fmt.Println( "Output of SenML failed:", err )
		return err
	}

	return nil
}




func main() {
	var err error
	
	flag.Parse()
	
	// load the input  
	msg, err := ioutil.ReadFile( flag.Arg(0) )
	if err != nil {
		fmt.Println("error reading SenML file",err)
		os.Exit( 1 )
	}	
	
	err = processData( msg )
}


