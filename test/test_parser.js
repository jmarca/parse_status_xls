
var should = require('should')
var parser = require('../lib/parse_xls_status.js')

describe('process xlsx file okay',function(){
    it('loads a file',function(done){

    })

})

var wb = load_file('../statusdocs/IRD 08-2013 MONTHLY SITE STATUSBA.xlsx')
var sheetnames =  wb.SheetNames
sheetnames.length.should.eql(1,'more than one sheet in file; quitting so as not to break anything')

var sheet = wb.Sheets[sheetnames[0]]


var next_col = (function _next_col(){
    var colnum = 0;
    var col;
    return function(_c){
        if(_c){
            colnum = _c
        }else{
            colnum++
        }
        col = XLS.utils.encode_col(colnum)
        return col;
    }
})()

function parse_header(sheet){
    // look at the first row in the sheet to determine the columns
    // with data I care about

    // either there are the "internal notes" columns, or not


    var candidate = {'site'         :'A'}

    // if we want the first month, then class status is 'd' else 'e'
    if(past_month){
        candidate.class_status = 'D'
    }else{
        candidate.class_status = 'E'
    }

    // class notes
    var col = next_col(5)
    console.log(col)
    col.should.eql('F')

    var class_notes = sheet[col+'1'].v
    console.log(sheet[col+1])
    class_notes.should.match(/class\s*notes/i)
    // or we croak
    candidate.class_notes = col

    // now is there an extra column here?
    col = next_col()

    if((/class\s*notes/i).test(sheet[col+'1'].v)){
        // add another column
        candidate.internal_class_notes = col
        col = next_col()
    }
    console.log(col)

    // load weight columns
    if(past_month){
        candidate.weight_status = col
        col = next_col() // skip the next month
    }else{
        col = next_col()
        candidate.weight_status = col
    }
    // now double check weight notes is correct
    col = next_col()
    console.log(col)
    var weight_notes = sheet[col+'1'].v
    weight_notes.should.match(/weight\s*notes/i)
    // or we croaked
    candidate.weight_notes = col

    // finally, check for internal weight notes column
    col = next_col()

    if((/weight\s*notes/i).test(sheet[col+'1'].v)){
        // add another column
        candidate.internal_weight_notes = col
    }
    return candidate
}

var past_month = true
var header_map = parse_header(sheet)
header_map.should.eql({ site: 'A',
                        class_status: 'D',
                        class_notes: 'F',
                        internal_class_notes: 'G',
                        weight_status: 'H',
                        weight_notes: 'J',
                        internal_weight_notes: 'K' }
                     )
past_month = false
header_map = parse_header(sheet)
header_map.should.eql({ site: 'A',
                        class_status: 'E',
                        class_notes: 'F',
                        internal_class_notes: 'G',
                        weight_status: 'I',
                        weight_notes: 'J',
                        internal_weight_notes: 'K' }
                     )
