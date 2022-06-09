from typedb.client import TypeDB, SessionType, TransactionType
import xml.etree.cElementTree as et

client = TypeDB.core_client(address=TypeDB.DEFAULT_ADDRESS)

def build_phone_call_graph(inputs):
    with TypeDB.core_client("localhost:1729") as client:
        with client.session("phone_calls", SessionType.DATA) as session:
            for input in inputs:
                print("Loading from [" + input["data_path"] + "] into TypeDB ...")
                load_data_into_typedb(input, session)

def load_data_into_typedb(input, session):
    items = parse_data_to_dictionaries(input)

    with session.transaction(TransactionType.WRITE) as transaction:
        for item in items:
            typeql_insert_query = input["template"](item)
            print("Executing TypeQL Query: " + typeql_insert_query)
            transaction.query().insert(typeql_insert_query)
        transaction.commit()

    print("\nInserted " + str(len(items)) + " items from [ " + input["data_path"] + "] into TypeDB.\n")

def company_template(company):
    return 'insert $company isa company, has name "' + company["name"] + '";'

def person_template(person):
    # insert person
    typeql_insert_query = 'insert $person isa person, has phone-number "' + person["phone_number"] + '"'
    if "first_name" in person:
        # person is a customer
        typeql_insert_query += ", has is-customer true"
        typeql_insert_query += ', has first-name "' + person["first_name"] + '"'
        typeql_insert_query += ', has last-name "' + person["last_name"] + '"'
        typeql_insert_query += ', has city "' + person["city"] + '"'
        typeql_insert_query += ", has age " + str(person["age"])
    else:
        # person is not a customer
        typeql_insert_query += ", has is-customer false"
    typeql_insert_query += ";"
    return typeql_insert_query

def contract_template(contract):
    # match company
    typeql_insert_query = 'match $company isa company, has name "' + contract["company_name"] + '";'
    # match person
    typeql_insert_query += ' $customer isa person, has phone-number "' + contract["person_id"] + '";'
    # insert contract
    typeql_insert_query += " insert (provider: $company, customer: $customer) isa contract;"
    return typeql_insert_query

def call_template(call):
    # match caller
    typeql_insert_query = 'match $caller isa person, has phone-number "' + call["caller_id"] + '";'
    # match callee
    typeql_insert_query += ' $callee isa person, has phone-number "' + call["callee_id"] + '";'
    # insert call
    typeql_insert_query += (" insert $call(caller: $caller, callee: $callee) isa call; " +
                           "$call has started-at " + call["started_at"] + "; " +
                           "$call has duration " + str(call["duration"]) + ";")
    return typeql_insert_query

def parse_data_to_dictionaries(input):
    items = []
    with open(input["data_path"] + ".xml", "rb") as inputfile:
        append = False
        for line in inputfile:
            if "<" + input["selector"] + ">" in str(line):
                ## start of a new xml tag
                buffer = line
                append = True
            elif "</" + input["selector"]  + ">" in str(line):
                ## we got a complete xml tag
                buffer += line
                append = False
                tnode = et.fromstring(buffer)
                ## parse the tag to a dictionary and append to tiems
                item = {}
                for element in tnode.getchildren():
                    item[element.tag] = element.text
                items.append(item)
                ## delete the buffer to free the memory
                del buffer
            elif append:
                ## inside the current xml tag
                buffer += line
    return items

inputs = [
    {
        "data_path": "/Users/remicarvalot/Desktop/StageCimpa/TestTypeDb/phone_calls/data/companies",
        "template": company_template,
        "selector": "company"
    },
    {
        "data_path": "/Users/remicarvalot/Desktop/StageCimpa/TestTypeDb/phone_calls/data/people",
        "template": person_template,
        "selector": "person"
    },
    {
        "data_path": "/Users/remicarvalot/Desktop/StageCimpa/TestTypeDb/phone_calls/data/contracts",
        "template": contract_template,
        "selector": "contract"
    },
    {
        "data_path": "/Users/remicarvalot/Desktop/StageCimpa/TestTypeDb/phone_calls/data/calls",
        "template": call_template,
        "selector": "call"
    }
]

build_phone_call_graph(inputs)