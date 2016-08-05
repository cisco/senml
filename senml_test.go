package senml_test

import (
	"encoding/base64"
	"fmt"
	"github.com/cisco/senml"
	"strconv"
	"testing"
)

func ExampleEncode() {
	v := 22.1
	s := senml.SenML{
		Records: []senml.SenMLRecord{
			senml.SenMLRecord{Value: &v, Unit: "degC", Name: "temp"},
		},
	}

	dataOut, err := senml.Encode(s, senml.JSON, senml.OutputOptions{})
	if err != nil {
		fmt.Println("Encode of SenML failed")
	} else {
		fmt.Println(string(dataOut))
	}
	// Output: [{"n":"temp","u":"degC","v":22.1}]
}

type TestVector struct {
	testDecode bool
	format     senml.Format
	binary     bool
	value      string
}

var testVectors = []TestVector{
	{true, senml.JSON, false, "W3siYm4iOiJkZXYxMjMiLCJidCI6LTQ1LjY3LCJidSI6ImRlZ0MiLCJidmVyIjo1LCJuIjoidGVtcCIsInUiOiJkZWdDIiwidCI6LTEsInV0IjoxMCwidiI6MjIuMSwicyI6MH0seyJuIjoicm9vbSIsInQiOi0xLCJ2cyI6ImtpdGNoZW4ifSx7Im4iOiJkYXRhIiwidmQiOiJhYmMifSx7Im4iOiJvayIsInZiIjp0cnVlfV0="},
	{true, senml.CBOR, true, "hKpiYm5mZGV2MTIzYmJ0+8BG1cKPXCj2YmJ1ZGRlZ0NkYnZlcgVhbmR0ZW1wYXP7AAAAAAAAAABhdPu/8AAAAAAA<AGF1ZGRlZ0NidXT7QCQAAAAAAABhdvtANhmZmZmZmqNhbmRyb29tYXT7v/AAAAAAAABidnNna2l0Y2hlbqJhbmRkYXRhYnZkY2FiY6JhbmJva2J2YvU="},
	{true, senml.XML, false, "PHNlbnNtbCB4bWxucz0idXJuOmlldGY6cGFyYW1zOnhtbDpuczpzZW5tbCI+PHNlbm1sIGJuPSJkZXYxMjMiIGJ0PSItNDUuNjciIGJ1PSJkZWdDIiBidmVyPSI1IiBuPSJ0ZW1wIiB1PSJkZWdDIiB0PSItMSIgdXQ9IjEwIiB2PSIyMi4xIiBzdW09IjAiPjwvc2VubWw+PHNlbm1sIG49InJvb20iIHQ9Ii0xIiB2cz0ia2l0Y2hlbiI+PC9zZW5tbD48c2VubWwgbj0iZGF0YSIgdmQ9ImFiYyI+PC9zZW5tbD48c2VubWwgbj0ib2siIHZiPSJ0cnVlIj48L3Nlbm1sPjwvc2Vuc21sPg=="},
	{false, senml.CSV, false, "dGVtcCwyNTU2OC45OTk5ODgsMjIuMTAwMDAwLGRlZ0MNCg=="},
	{true, senml.MPACK, true, "lIqiYm6mZGV2MTIzomJ0y8BG1cKPXCj2omJ1pGRlZ0OkYnZlcgWhbqR0ZW1woXPLAAAAAAAAAAChdMu/8AAAAAAAAKF1pGRlZ0OidXTLQCQAAAAAAAChdstANhmZmZmZmoOhbqRyb29toXTLv/AAAAAAAACidnOna2l0Y2hlboKhbqRkYXRhonZko2FiY4KhbqJva6J2YsM="},
	{false, senml.LINEP, false, "Zmx1ZmZ5U2VubWwsbj10ZW1wLHU9ZGVnQyB2PTIyLjEgLTEwMDAwMDAwMDAK"},
}

func TestEncode(t *testing.T) {
	value := 22.1
	sum := 0.0
	vb := true
	s := senml.SenML{
		Records: []senml.SenMLRecord{
			senml.SenMLRecord{BaseName: "dev123",
				BaseTime:    -45.67,
				BaseUnit:    "degC",
				BaseVersion: 5,
				Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
			senml.SenMLRecord{StringValue: "kitchen", Name: "room", Time: -1.0},
			senml.SenMLRecord{DataValue: "abc", Name: "data"},
			senml.SenMLRecord{BoolValue: &vb, Name: "ok"},
		},
	}
	options := senml.OutputOptions{Topic: "fluffySenml", PrettyPrint: false }
	for i, vector := range testVectors {

		dataOut, err := senml.Encode(s, vector.format, options)
		if err != nil {
			t.Fail()
		}
		if vector.binary {
			fmt.Print("Test Encode " + strconv.Itoa(i) + " got: ")
			fmt.Println(dataOut)
		} else {
			fmt.Println("Test Encode " + strconv.Itoa(i) + " got: " + string(dataOut))
		}

		if base64.StdEncoding.EncodeToString(dataOut) != vector.value {
			t.Error("Failed Encode for format " + strconv.Itoa(i) + " got: " + base64.StdEncoding.EncodeToString(dataOut))
		}
	}

}

func TestDecode(t *testing.T) {
	for i, vector := range testVectors {
		if vector.testDecode {
			data, err := base64.StdEncoding.DecodeString(vector.value)
			if err != nil {
				t.Fail()
			}

			s, err := senml.Decode(data, vector.format)
			if err != nil {
				t.Fail()
			}

			dataOut, err := senml.Encode(s, senml.JSON, senml.OutputOptions{PrettyPrint: true})
			if err != nil {
				t.Fail()
			}

			fmt.Println("Test Decode " + strconv.Itoa(i) + " got: " + string(dataOut))
		}
	}
}

func TestNormalize(t *testing.T) {
	value := 22.1
	sum := 0.0
	vb := true
	s := senml.SenML{
		Records: []senml.SenMLRecord{
			senml.SenMLRecord{BaseName: "dev123/",
				BaseTime:    897845.67,
				BaseUnit:    "degC",
				BaseVersion: 5,
				Value:       &value, Unit: "degC", Name: "temp", Time: -1.0, UpdateTime: 10.0, Sum: &sum},
			senml.SenMLRecord{StringValue: "kitchen", Name: "room", Time: -1.0},
			senml.SenMLRecord{DataValue: "abc", Name: "data"},
			senml.SenMLRecord{BoolValue: &vb, Name: "ok"},
		},
	}

	n := senml.Normalize(s)

	dataOut, err := senml.Encode(n, senml.JSON, senml.OutputOptions{PrettyPrint: true})
	if err != nil {
		t.Fail()
	}
	fmt.Println("Test Normalize got: " + string(dataOut))
	
	if base64.StdEncoding.EncodeToString(dataOut) != "WwogIHsKICAgICJidmVyIjogNSwKICAgICJuIjogImRldjEyMy90ZW1wIiwKICAgICJ1IjogImRlZ0MiLAogICAgInQiOiA4OTc4NDQuNjcsCiAgICAidXQiOiAxMCwKICAgICJ2IjogMjIuMSwKICAgICJzIjogMAogIH0sCiAgewogICAgImJ2ZXIiOiA1LAogICAgIm4iOiAiZGV2MTIzL3Jvb20iLAogICAgInUiOiAiZGVnQyIsCiAgICAidCI6IDg5Nzg0NC42NywKICAgICJ2cyI6ICJraXRjaGVuIgogIH0sCiAgewogICAgImJ2ZXIiOiA1LAogICAgIm4iOiAiZGV2MTIzL29rIiwKICAgICJ1IjogImRlZ0MiLAogICAgInQiOiA4OTc4NDUuNjcsCiAgICAidmIiOiB0cnVlCiAgfQpd" {
		t.Error("Failed Normalize got: " + base64.StdEncoding.EncodeToString(dataOut) )
	}
}
