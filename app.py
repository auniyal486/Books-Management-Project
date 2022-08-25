from flask import Flask,render_template, request, url_for, redirect
import boto3
import re
import datetime
import json
import os

app = Flask(__name__)
dynamo_client = boto3.resource('dynamodb',aws_access_key_id=os.environ.get("AWS_DYNAMO_KEY"), aws_secret_access_key=os.environ.get("DYNAMO_SECRET"), region_name=os.environ.get('REGION_NAME'))

def get_dynamo_client():
    return dynamo_client

@app.route('/')
def index():
    return json.dumps("Books Management Project")

@app.route('/get-books',methods=['GET'])
def get_books():
    args = request.args
    book=args['book']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Books')
    response = table.scan(
        FilterExpression= 'contains(book_name, :book)',
        ExpressionAttributeValues={
            ':book': book
        }
    )

    return json.dumps(response['Items'])

@app.route('/get-books-on-range',methods=['GET'])
def get_books_on_range():
    args = request.get_json()
    min_range=args['min']
    max_range=args['max']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Books')
    response = table.scan(
        FilterExpression= 'rent_per_day>=:min and rent_per_day<=:max',
        ExpressionAttributeValues={
            ':min': min_range,
            ':max':max_range
        }
    )
    return json.dumps(response['Items'])

@app.route('/get-books-on-condition',methods=['GET'])
def get_books_on_condition():
    data = request.get_json()
    min_range=data['range']['min']
    max_range=data['range']['max']
    book=data['book']
    category=data['category']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Books')
    response = table.scan(
        FilterExpression= 'rent_per_day>=:min and rent_per_day<=:max and contains(book_name, :book) and contains(category, :cat)',
        ExpressionAttributeValues={
            ':min': min_range,
            ':max':max_range,
            ':book':book,
            ':cat':category
        }
    )
    return json.dumps(response['Items'])

@app.route('/update-issue-transaction',methods=["POST"])
def update_issue_transaction():
    data = request.get_json()
    book=data['book']
    person=data['person']
    issue_date= data['issue_date']
    dynamo_client = get_dynamo_client()
    table1 = dynamo_client.Table('Books')
    table2 = dynamo_client.Table('Transactions')
    response = table1.get_item(
        Key={
            'book_name':book
        }
    )
    if 'Item' in response:
        try:
            rent_per_day=response['Item']['rent_per_day']
            response = table2.put_item(
                Item = {
                    'book_name':book,
                    'person_name':person,
                    'issue_date': issue_date,
                    'return_date':None,
                    'rent':0,
                    'rent_per_day':rent_per_day
                }
            )
            return json.dumps(True)
        except:
            return json.dumps(False)
    else:
        return json.dumps(False)


@app.route('/update-return-transaction',methods=["POST"])
def update_return_transaction():
    data = request.get_json()
    book=data['book']
    person=data['person']
    return_date=data['return_date']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Transactions')
    response = table.get_item(
        Key={
            'book_name': book,
            'person_name': person
        }
    )
    item =response['Item']
    is_date=datetime.datetime.strptime(item['issue_date'], '%Y-%m-%d')
    re_date=datetime.datetime.strptime(return_date, '%Y-%m-%d')
    cur_rent=(re_date-is_date).days*item['rent_per_day']
    item['rent']+=cur_rent
    item['return_date']=return_date
    response = table.put_item(
        Item=item
    )
    return json.dumps(str(cur_rent))

@app.route('/people-issued-book',methods=["GET"])
def people_issued_book():
    args = request.args
    book=args['book']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Transactions')
    response = table.scan(
        FilterExpression = 'book_name = :book',
        ExpressionAttributeValues={
            ':book': book
        }
    )
    people=[]
    for item in response['Items']:
        people.append(item['person_name'])
    return json.dumps(str(people))

@app.route('/total-rent-of-book',methods=["GET"])
def total_rent_of_book():
    args = request.args
    book=args['book']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Transactions')
    response = table.scan(
        FilterExpression = 'book_name = :book',
        ExpressionAttributeValues={
            ':book': book
        }
    )
    rent=0
    for item in response['Items']:
        rent+=item['rent']
    return json.dumps(str(rent))

@app.route('/books-issued-by-person')
def books_issued_by_person():
    args = request.args
    person=args['person']
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Transactions')
    response = table.scan(
        FilterExpression = 'person_name = :person',
        ExpressionAttributeValues={
            ':person': person
        }
    )
    books=[]
    for item in response['Items']:
        books.append(item['book_name'])
    return json.dumps(str(books))

@app.route('/get-issued-information-on-date-range',methods=['GET'])
def get_issued_information_on_date_range():
    data = request.get_json()
    start_date=datetime.datetime.strptime(data['start_date'], '%Y-%m-%d')
    end_date=datetime.datetime.strptime(data['end_date'], '%Y-%m-%d')
    dynamo_client = get_dynamo_client()
    table = dynamo_client.Table('Transactions')
    response = table.scan()
    books=[]
    for item in response['Items']:
        issue_date=datetime.datetime.strptime(item['issue_date'],'%Y-%m-%d')
        if start_date<=issue_date and end_date>=issue_date:
            books.append(item)
    return json.dumps(str(books))

if __name__=="__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(port=port)