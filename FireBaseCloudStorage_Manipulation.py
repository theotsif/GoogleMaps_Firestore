import re
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

def Write_FireCloud_Documents(Firebase_Admin_SDK, FireBase_path, data, delete=False):
    """
        Write to Cloud Firestore. Allows deleting.

        :data: list of dicts
    """

    cred = credentials.Certificate(Firebase_Admin_SDK)
    try:
        firebase_admin.initialize_app(cred)
    except:
        pass

    path = re.split('/', FireBase_path)
    len_path = len(path)
    quotient = len_path // 2

    if isinstance(data, dict):
        types_list = ['collection(', 'document('] * quotient
        full_path = list(zip(types_list, path))
        full_path = ''.join(['.' + i[0] + "'" + i[1] + "'" + ')' for i in full_path])
        call = "firestore.client()" + full_path + ".set(input))"
        eval(call)

    elif isinstance(data, list):
        first = '.collection(' + "'" + path[0] + "'" + ')'
        types_list = ['document(', 'collection('] * quotient
        full_path = list(zip(types_list, path[1:]))
        full_path = ''.join(['.' + i[0] + "'" + i[1] + "'" + ')' for i in full_path])
        full_path = first + full_path

        batch = firestore.client().batch()

        for doc in data:
            if delete:
                call = 'firestore.client()' + full_path + '.document("' + doc['name'] + '")'
                batch.delete(eval(call))
            else:
                call = 'firestore.client()' + full_path + '.document("' + doc['name'] + '")'
                batch.set(eval(call), doc)

        # Commit the batch
        batch.commit()

    else:
        raise ValueError('Wrong input type')


def Update_FireCloud_Documents_fields(Firebase_Admin_SDK, FireBase_path, data, field_name, delete=False):
    """
        Update Cloud Firestore. It does not allow deleting.

        data: list of dicts
    """
    cred = credentials.Certificate(Firebase_Admin_SDK)
    try:
        firebase_admin.initialize_app(cred)
    except:
        pass

    path = re.split('/', FireBase_path)
    len_path = len(path)
    quotient = len_path // 2

    if isinstance(data, list):
        first = '.collection(' + "'" + path[0] + "'" + ')'
        types_list = ['document(', 'collection('] * quotient
        full_path = list(zip(types_list, path[1:]))
        full_path = ''.join(['.' + i[0] + "'" + i[1] + "'" + ')' for i in full_path])
        full_path = first + full_path

        batch = firestore.client().batch()

        keys_remove = ['FireBaseCloud_id']
        for doc in data:
            print(doc['FireBaseCloud_id'])
            call = 'firestore.client()' + full_path + '.document(' + "'" + doc['FireBaseCloud_id'] + "'" + ')'
            doc = {k: doc[k] for k in doc if k not in keys_remove}

            if delete:
                fields = dict((i, firestore.DELETE_FIELD) for i in list(doc.keys()))
                eval(call).update(fields)
            else:
                print(call)
                batch.update(eval(call), {field_name: doc})

        # Commit the batch
        batch.commit()

    else:
        raise ValueError('Wrong input type')


def Read_FireCloud_Collection_Documents(Firebase_Admin_SDK, FireBase_path):
    """
    Read from Cloud Firestore.

    data: list of dicts
    """

    cred = credentials.Certificate(Firebase_Admin_SDK)

    try:
        firebase_admin.initialize_app(cred)
    except:
        pass

    path = re.split('/', FireBase_path)
    len_path = len(path)
    quotient = len_path // 2

    if len_path % 2 != 0:

        if len_path == 1:
            full_path = 'collection(' + "'" + path[0] + "'" + ')'

        else:
            first = '.collection(' + "'" + path[0] + "'" + ')'
            types_list = ['document(', 'collection('] * quotient
            full_path = list(zip(types_list, path[1:]))
            full_path = ''.join(['.' + i[0] + "'" + i[1] + "'" + ')' for i in full_path])
            full_path = first + full_path

        call = "firestore.client()" + full_path + ".get()"
        collection = eval(call)
        ids = [doc.id for doc in collection]
        collection = [doc.to_dict() for doc in collection]
        for i in range(len(ids)):
            collection[i].update({'FireBaseCloud_id': ids[i]})
        return collection

    else:
        types_list = ['collection(', 'document('] * quotient
        full_path = list(zip(types_list, path))
        full_path = ''.join(['.' + i[0] + "'" + i[1] + "'" + ')' for i in full_path])
        call = "firestore.client()" + full_path + ".get()"
        document = eval(call)
        document = document.to_dict()

        return document


