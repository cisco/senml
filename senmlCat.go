package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"flag"
	"fmt"
	"github.com/ugorji/go/codec"
	"hash/crc32"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"
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
var kafkaUrl = flag.String("kafka", "", "URL to for Apache Kafka Broker to send data to")

var kafkaTopic = flag.String("topic", "senml", "Apache Kafka topic")

var doJsonPtr = flag.Bool("json", false, "output JSON formatted SenML ")
var doCborPtr = flag.Bool("cbor", false, "output CBOR formatted SenML ")
var doXmlPtr  = flag.Bool("xml",  false, "output XML formatted SenML ")
var doCsvPtr  = flag.Bool("csv",  false, "output CSV formatted SenML ")
var doMpackPtr = flag.Bool("mpack", false, "output MessagePack formatted SenML ")
var doLinpPtr = flag.Bool("linp", false, "output InfluxDB LineProtcol formatted SenML ")

var doIJsonPtr = flag.Bool("ijson", false, "input JSON formatted SenML ")
var doIXmlPtr = flag.Bool("ixml", false, "input XML formatted SenML ")
var doICborPtr = flag.Bool("icbor", false, "input CBOR formatted SenML ")
var doIMpackPtr = flag.Bool("impack", false, "input MessagePack formatted SenML ")

var kafkaConn net.Conn = nil
var kafkaReqNumber uint32 = 1

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
			fmt.Println("error parsing JSON SenML",err)
			return s,err
		}
	}
	
	// parse the input XML
	if ( *doIXmlPtr ) {
		err = xml.Unmarshal(msg, &s)
		if err != nil {
			fmt.Println("error parsing XML SenML",err)
			return s,err
		}
	}
	
	// parse the input CBOR
	if ( *doICborPtr ) {
		var cborHandle codec.Handle = new( codec.CborHandle )
		var decoder *codec.Decoder = codec.NewDecoderBytes( msg, cborHandle )
		err = decoder.Decode( &s.Records )
		if err != nil {
			fmt.Println("error parsing CBOR SenML",err)
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
			fmt.Println("error parsing MPACK SenML",err)
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
			fmt.Println("error encoding JSON SenML",err)
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
			fmt.Println("error encoding XML SenML",err);
			return nil, err
		}
	}

	// output a CSV version 
	if ( *doCsvPtr ) {
		var lines string
		for _,r := range s.Records {
			if r.Value != nil {
				lines += fmt.Sprintf( "%s,", r.Name )
				lines += fmt.Sprintf( "%f,", (r.Time / (24.0*3600.0) ) + 25569 ) // excell time in days since 1900, unix seconds since 1970
				// ( 1970 is 25569 days after 1900 )
				lines += fmt.Sprintf( "%f,", *r.Value )
				lines += fmt.Sprintf( "%s\r\n", r.Unit )
			}
		}
		data = []byte( lines )
		
		if err != nil {
			fmt.Println("error encoding CSV SenML",err);
			return nil, err
		}
	}
	// output a CBOR version 
	if ( *doCborPtr ) {
		var cborHandle codec.Handle = new(codec.CborHandle)
		var encoder *codec.Encoder = codec.NewEncoderBytes( &data, cborHandle)
		err = encoder.Encode( s.Records )
		if err != nil {
			fmt.Println("error encoding CBOR SenML",err)
			return nil, err
		}
	}

	// output a MPACK version 
	if ( *doMpackPtr ) {
		var mpackHandle codec.Handle = new(codec.MsgpackHandle)
		var encoder *codec.Encoder = codec.NewEncoderBytes( &data, mpackHandle)
		err = encoder.Encode( s.Records )
		if err != nil {
			fmt.Println("error encoding MPACK SenML",err)
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
				lines += fmt.Sprintf( " %f\n", r.Time )
			}
		}
		data = []byte( lines )
	}
	
	if ( *doSizePtr ) {
		fmt.Println("Output message size = ", len( data ) )
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
		fmt.Println( string( data ) )
	}

	if kafkaConn != nil  {
		//fmt.Println( "sending to Kafka=<" + string(data) + ">" )

		topic := []byte( string(*kafkaTopic) )
		clientName := []byte( "SenMLCat-0.1" )
		partition := 0 // TODO - does this need to be settable from CLI 
		reqID := kafkaReqNumber; kafkaReqNumber += 1
		
		clientNameLen  := len( clientName ) 
		topicLen := len( topic ) 
		dataLen := len( data ) 
		
		var totalLen int = 8 + 56 + clientNameLen + topicLen + dataLen // TODO 
		msg := make( []byte, totalLen )

		// protcol doc at https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
		l := 0
		// generic size header for req or response
		totalLenLoc := l; l += 4 // request size after theses bytes
		
		// generic request header 
		binary.BigEndian.PutUint16(msg[l:], uint16(0) ); l += 2 // ApiKey - int16 (ProduceRequest = 0)
		binary.BigEndian.PutUint16(msg[l:], uint16(1) ); l += 2 // ApiVersion  - int16 (curent = 0 but code uses 1 TODO)
		binary.BigEndian.PutUint32(msg[l:], uint32(reqID) ); l += 4 // CorrelationID - int32
		binary.BigEndian.PutUint16(msg[l:], uint16( clientNameLen ) ); l += 2 // ClientID Length - int16 
		copy( msg[l:], clientName ); l += clientNameLen // ClientID
		
		// produce request header 
		binary.BigEndian.PutUint16(msg[l:], uint16( 1 ) ); l += 2 // ReqAcks - int16 TODO 
		binary.BigEndian.PutUint32(msg[l:], uint32( 1500 ) ); l += 4 // Timeout - int32 - in milli seconds
		binary.BigEndian.PutUint32(msg[l:], uint32( 1 ) ); l += 4  // Array Lenght (for each TopicName) 
		binary.BigEndian.PutUint16(msg[l:], uint16( topicLen ) ); l += 2 // topic length - int16
		copy( msg[l:], topic ); l += topicLen // topic 
		binary.BigEndian.PutUint32(msg[l:], uint32( 1 ) ); l += 4  // Array Length (for each Partition) 
		binary.BigEndian.PutUint32(msg[l:], uint32( partition ) ); l += 4 // Partion - int32
		msgSetLenLoc := l; l += 4 // msg set size - int32

		// message Set Header
		binary.BigEndian.PutUint64(msg[l:], uint64( 0 ) ); l += 8 // offset - int64 - any value in produce
		msgLenLoc := l; l += 4 // message size - int32 
		
		// message
		checksumLoc := l; l += 4 // CRC - intt32 - compute over rest of message
		msg[l] = uint8(0); l += 1  // magic - int8 - current = 0 
		msg[l] = uint8(0); l += 1 // attributes - int8 - value =0
		
		// key - bytes - can be null
		binary.BigEndian.PutUint32(msg[l:], uint32( 0xFFFFFFFF ) ); l += 4 // key length - int32 - (-1 for NULL)

		// value - bytes
		binary.BigEndian.PutUint32(msg[l:], uint32( dataLen ) ); l += 4 // data length - int32 
		copy( msg[l:], data ); l += dataLen // message data

		// backpatch in lengths
		binary.BigEndian.PutUint32(msg[msgLenLoc:], uint32( l - msgLenLoc - 4  ) );
		binary.BigEndian.PutUint32(msg[msgSetLenLoc:], uint32( l - msgSetLenLoc - 4  ) );
		binary.BigEndian.PutUint32(msg[totalLenLoc:], uint32( l - totalLenLoc - 4 )  )
				
		// backpatch in checksum
		checksum := crc32.ChecksumIEEE( msg[checksumLoc+4:l] ) 
		binary.BigEndian.PutUint32(msg[checksumLoc:], uint32(checksum) )
		
		if ( l != totalLen ) {
			fmt.Println( "Header Len ",  l-(clientNameLen + topicLen + dataLen) )
			panic( "assumed kafka mesg header length wrong " )
		}

		n,err := kafkaConn.Write( msg )
		if err != nil {
			fmt.Println( "Write to kafka ",  string(*kafkaUrl) ," got error", err )
			return err
		}
		if n != totalLen {
			return errors.New( "Coud not write request to Kafka server" ) // TODO 
		}

		// read a length
		lenBuf  := make( []byte, 4 )
		n,err = kafkaConn.Read( lenBuf )
		if err != nil {
			return errors.New( "Coud not read response from Kafka server" )
		}
		if n != 4 {
			return errors.New( "Coud not read response from Kafka server" )
		}

		respLen := binary.BigEndian.Uint32(lenBuf)
		buf := make( []byte, respLen )

		n,err = kafkaConn.Read( buf )
		if err != nil {
			return errors.New( "Coud not read response from Kafka server" )
		}
		if n != int(respLen) {
			return errors.New( "Coud not read response from Kafka server" )
		}

		l = 0
		respReqID := binary.BigEndian.Uint32(buf[l:]); l+= 4 	         // CorrelationID int32
		respNumTopics := binary.BigEndian.Uint32(buf[l:]); l+= 4         // Aray Length - int32 (for each topic)
		for t:=uint32(0); t<respNumTopics; t += 1 {
			topicLen := int(binary.BigEndian.Uint16(buf[l:])); l+= 2          // TopicName length - int16
			topic := string( buf[l:l+topicLen] ); l += topicLen          // Topic string
			respNumPartitions := binary.BigEndian.Uint32(buf[l:]); l+= 4 // Array length - int32 (for each partition)
			for p:=uint32(0); p<respNumPartitions; p += 1 {
				respPartition := binary.BigEndian.Uint32(buf[l:]); l+= 4 // Partition - int32
				respError := binary.BigEndian.Uint16(buf[l:]); l+= 2	 // Error code - int16 ( 0 is no error)
				respOffset := binary.BigEndian.Uint64(buf[l:]); l+= 8 	 // offset - int64

				if respError != 0 {
					fmt.Println( "Kafka response err=",respError,
						" part=",respPartition, " topic=", topic, " respReqID=",respReqID, " respOffset=",respOffset  )

					switch respError {
					case 0: // no error
					case 0xFFFF: return errors.New( "Kafka unknown error" )// unknown error
					case 1: return errors.New( "Kafka Error OffsetOutOfRange" )
					case 2: return errors.New( "Kafka Error InvalidMessage" )
					case 3: return errors.New( "Kafka Error UnknownTopicOrPartition" )
					case 4: return errors.New( "Kafka Error InvalidMessageSize" )
					case 5: return errors.New( "Kafka Error LeaderNotAvailable" )
					case 6: return errors.New( "Kafka Error NotLeaderForPartition" )
					case 7: return errors.New( "Kafka Error RequestTimedOut" )
					case 8: return errors.New( "Kafka Error BrokerNotAvailable" )
					case 9: return errors.New( "Kafka Error ReplicaNotAvailable" )
					case 10: return errors.New( "Kafka Error MessageSizeTooLarge" )
					case 11: return errors.New( "Kafka Error StaleControllerEpochCode" )
					case 12: return errors.New( "Kafka Error OffsetMetadataTooLargeCode" )
					case 14: return errors.New( "Kafka Error GroupLoadInProgressCode" )
					case 15: return errors.New( "Kafka Error GroupCoordinatorNotAvailableCode" )
					case 16: return errors.New( "Kafka Error NotCoordinatorForGroupCode" )
					case 17: return errors.New( "Kafka Error InvalidTopicCode" )
					case 18: return errors.New( "Kafka Error RecordListTooLargeCode" )
					case 19: return errors.New( "Kafka Error NotEnoughReplicasCode" )
					case 20: return errors.New( "Kafka Error NotEnoughReplicasAfterAppendCode" )
					case 21: return errors.New( "Kafka Error InvalidRequiredAcksCode" )
					case 22: return errors.New( "Kafka Error IllegalGenerationCode" )
					case 23: return errors.New( "Kafka Error InconsistentGroupProtocolCode" )
					case 24: return errors.New( "Kafka Error InvalidGroupIdCode" )
					case 25: return errors.New( "Kafka Error UnknownMemberIdCode" )
					case 26: return errors.New( "Kafka Error InvalidSessionTimeoutCode" )
					case 27: return errors.New( "Kafka Error RebalanceInProgressCode" )
					case 28: return errors.New( "Kafka Error InvalidCommitOffsetSizeCode" )
					case 29: return errors.New( "Kafka Error TopicAuthorizationFailedCode" )
					case 30: return errors.New( "Kafka Error GroupAuthorizationFailedCode" )
					case 31: return errors.New( "Kafka Error ClusterAuthorizationFailedCode" )
					}
				}
				
			}
		}
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
	var senml SenML
	var err error
	
	//fmt.Println( "DataIn:", dataIn )
	
	senml,err = decodeSenMLTimed( dataIn )
	if err != nil {
		fmt.Println( "Decode of SenML failed" )
		return err
	}

	//fmt.Println( "Senml:", senml.Records )
	if *doExpandPtr {
		senml = expandSenML( senml )
	}
	
	var dataOut []byte;
	dataOut, err = encodeSenML( senml )
	if err != nil {
		fmt.Println( "Encode of SenML failed" )
		return err
	}

	//fmt.Println( "DataOut:", dataOut )

	err = outputData( dataOut )
	if err != nil {
		fmt.Println( "Output of SenML failed:", err )
		return err
	}

	return nil
}


func httpReqHandler(w http.ResponseWriter, r *http.Request) {
	//fmt.Println( "HTTP request to ",  r.URL.Path )
	//fmt.Println( "Method: ",  r.Method )

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
	var err error
	
	flag.Parse()

	if len(*kafkaUrl) != 0  {
		kafkaConn, err = net.DialTimeout("tcp", *kafkaUrl , 2500 * time.Millisecond )
		if err != nil {
			fmt.Println("error connecting to kafka broker",err)
			os.Exit( 1 )
		}
		defer kafkaConn.Close()
	}
	
	if  *httpPort != 0 {
		http.HandleFunc("/", httpReqHandler)
		http.ListenAndServe(":"+strconv.Itoa(*httpPort), nil)
	} else {
		// load the input  
		msg, err := ioutil.ReadFile( flag.Arg(0) )
		if err != nil {
			fmt.Println("error reading SenML file",err)
			os.Exit( 1 )
		}	
		//fmt.Println(string(msg))
		
		err = processData( msg )
	}
}


