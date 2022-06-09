from typedb.client import TypeDB, SessionType
from typedb.client import TransactionType


#Afin de charger les données de chaque fichier dans TypeDB, nous devons :

#récupérer une liste contenant des dictionnaires, chacun d'entre eux représentant un élément de données. 
# Nous le faisons en appelant parse_data_to_dictionaries(input)
# pour chaque dictionnaire dans les items : 
# a) créer une transaction, qui se ferme une fois utilisée, 
# b) construire la requête typeql_insert_query en utilisant la fonction template correspondante, 
# c) exécuter la requête et d)commit la transaction.


def load_data_into_typedb(input, session):
    items = parse_data_to_dictionaries(input)

    for item in items:
        with session.transaction(TransactionType.WRITE) as transaction:
            typeql_insert_query = input["template"](item)
            print("Executing TypeQL Query: " + typeql_insert_query)
            transaction.query().insert(typeql_insert_query)
            transaction.commit()

    print("\nInserted " + str(len(items)) + " items from [ " + input["data_path"] + "] into TypeDB.\n")


# C'est la principale et seule fonction que nous devons appeler pour commencer à charger les données dans TypeDB.

# Ce qui se passe dans cette fonction, est le suivant :

# Un client TypeDB est créé, connecté au serveur que nous avons en local.
# Une session est créée, connectée à la base de données phone_calls. 
# Notez qu'en utilisant with, nous indiquons que la session se ferme après avoir été utilisée.
# Pour chaque dictionnaire d'entrée dans inputs, nous appelons la fonction load_data_into_typedb(input, session). 
# Ceci s'occupe de charger les données telles que spécifiées dans le dictionnaire d'entrée dans notre base de données.


def build_phone_call_graph(inputs):
    with TypeDB.core_client("localhost:1729") as client:
        with client.session("phone_calls", SessionType.DATA) as session:
            for input in inputs:
                print("Loading from [" + input["data_path"] + "] into TypeDB ...")
                load_data_into_typedb(input, session)



#Les modèles sont des fonctions simples qui acceptent un dictionnaire, représentant un seul élément de données. 
# Les valeurs de ce dictionnaire remplissent les espaces vides du modèle de requête. 
# Le résultat est une requête d'insertion TypeQL. Nous avons besoin de 4 de ces modèles. 
# Passons-les en revue un par un.

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