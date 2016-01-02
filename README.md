# senmlCat
Tool to convert SenML between formats and act as gateway server to other services

# usage

## convert JSON SenML to XML 
senmlCat -json -i data.json > data.xml

## convert JSON SenML to CBOR
senmlCat.go -ijson -cbor data.json > data.cbor 

## convert to Excel spreadsheet CSV file
go run senmlCat.go -expand -ijsons -csv -print data.json > foo.csv

Note that this moves times to excel times that are days since 1900

