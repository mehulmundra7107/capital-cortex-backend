from fastapi import FastAPI, Body
from pydantic import BaseModel
from fastapi.encoders import jsonable_encoder
from typing import Optional
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
import pymongo
import requests
import json
import pandas as pd
from bson.objectid import ObjectId
import time
import enum
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv

from ratings import competitor_analysis, oppurtunity_rating, sectoral_analysis, relative_prosperity, ease_of_business

load_dotenv()

app = FastAPI(title="Capital Cortex API")

# Load from environment variables
MONGODB_URL = os.getenv('MONGODB_URL')
BORROWER_CLIENT_URL = os.getenv('BORROWER_CLIENT_URL', 'http://localhost:5174')
SETU_CLIENT_ID = os.getenv('SETU_CLIENT_ID')
SETU_CLIENT_SECRET = os.getenv('SETU_CLIENT_SECRET')

# MongoDB connection
client = pymongo.MongoClient(MONGODB_URL)
db = client['capitalcortex']

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class BorrowerModel(BaseModel):
    name: str
    mobileNumber: str
    typeOfBusiness: str
    businessAddress: str
    businessState: str
    businessDistrict: str
    businessPinCode: str
    amountApplied: int
    amountApproved: Optional[int] = 0
    consentId: Optional[str] = None
    sessionId: Optional[str] = None
    applicationStatus: int


class ApplicationStatus(enum.Enum):
    newApplication = 0
    accepted = 1
    halted = 3
    rejected = 2


def get_consent(mobileNumber, userid):
    url = "https://fiu-uat.setu.co/consents"

    current_datetime = datetime.now()
    one_month_later = current_datetime + relativedelta(months=1)

    payload = json.dumps({
        "Detail": {
            "consentStart": current_datetime.isoformat() + '+05:30',
            "consentExpiry": one_month_later.isoformat() + '+05:30',
            "Customer": {
                "id": f"{mobileNumber}@onemoney"
            },
            "FIDataRange": {
                "from": (current_datetime - relativedelta(years=1)).strftime('%Y-%m-%dT00:00:00Z'),
                "to": current_datetime.strftime('%Y-%m-%dT00:00:00Z')
            },
            "consentMode": "STORE",
            "consentTypes": [
                "TRANSACTIONS",
                "PROFILE",
                "SUMMARY"
            ],
            "fetchType": "PERIODIC",
            "Frequency": {
                "value": 30,
                "unit": "MONTH"
            },
            "DataLife": {
                "value": 1,
                "unit": "MONTH"
            },
            "DataConsumer": {
                "id": "setu-fiu-id"
            },
            "Purpose": {
                "Category": {
                    "type": "string"
                },
                "code": "101",
                "text": "Loan underwriting",
                "refUri": "https://api.rebit.org.in/aa/purpose/101.xml"
            },
            "fiTypes": [
                "DEPOSIT"
            ]
        },
        "redirectUrl": f"{BORROWER_CLIENT_URL}/confirmscreen/{userid}"
    })

    headers = {
        'x-client-id': SETU_CLIENT_ID,
        'x-client-secret': SETU_CLIENT_SECRET,
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    return response.json()


def create_data_session(consent_id):
    url = "https://fiu-uat.setu.co/sessions"

    current_datetime = datetime.now()

    payload = json.dumps({
        "consentId": consent_id,
        "DataRange": {
            "from": (current_datetime - relativedelta(years=1)).strftime('%Y-%m-%dT00:00:00.000Z'),
            "to": current_datetime.strftime('%Y-%m-%dT00:00:00.000Z')
        },
        "format": "json"
    })

    headers = {
        'x-client-id': SETU_CLIENT_ID,
        'x-client-secret': SETU_CLIENT_SECRET,
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    session_id = response.json()['id']
    return session_id


def fetch_and_save_session_data(userid, sessionid):
    url = f"https://fiu-uat.setu.co/sessions/{sessionid}"

    headers = {
        'x-client-id': SETU_CLIENT_ID,
        'x-client-secret': SETU_CLIENT_SECRET
    }

    response = requests.request("GET", url, headers=headers)
    session_status = response.json()['status']

    if session_status == "COMPLETED":
        payload = response.json()['Payload'][0]
        account_details = payload['data'][0]['decryptedFI']['account']
        obj = {
            'userid': userid,
            'accountDetails': account_details
        }
        result = db['financialData'].insert_one(obj)
        return {'status': session_status, 'db_inserted_id': str(result.inserted_id)}
    else:
        return {'status': session_status}


def transaction_analysis(userid):
    try:
        financial_details = db['financialData'].find_one({'userid': userid})
        if not financial_details:
            raise ValueError("No financial data found")

        account_details = financial_details['accountDetails']
        transactions = account_details.get('transactions', {}).get('Transaction', [])

        online_spends = 0
        cash_withdrawal = 0
        earnings = 0

        for txn in transactions:
            amount = float(txn.get('amount', 0))
            txn_type = txn.get('type', '').upper()
            narration = txn.get('narration', '').lower()

            if txn_type == 'CREDIT':
                earnings += amount
            elif 'atm' in narration or 'cash' in narration:
                cash_withdrawal += amount
            else:
                online_spends += amount

        total = online_spends + cash_withdrawal + earnings
        if total == 0:
            return [
                {'name': 'Online Spends', 'value': 30},
                {'name': 'Earnings', 'value': 50},
                {'name': 'Cash Withdrawal', 'value': 20}
            ]

        return [
            {'name': 'Online Spends', 'value': round((online_spends / total) * 100, 2)},
            {'name': 'Earnings', 'value': round((earnings / total) * 100, 2)},
            {'name': 'Cash Withdrawal', 'value': round((cash_withdrawal / total) * 100, 2)}
        ]

    except Exception as e:
        print(f"transaction_analysis error: {e}")
        return [
            {'name': 'Online Spends', 'value': 30},
            {'name': 'Earnings', 'value': 50},
            {'name': 'Cash Withdrawal', 'value': 20}
        ]


@app.get('/')
def index():
    return {'msg': 'Capital Cortex API is running. Check /docs for more'}


@app.post('/addborrower')
def add_borrower(borrower: BorrowerModel = Body(...)):
    borrower = jsonable_encoder(borrower)
    borrower['businessState'] = borrower['businessState'].lower()
    borrower['businessDistrict'] = borrower['businessDistrict'].lower()

    result = db["borrowers"].insert_one(borrower)
    userid = result.inserted_id

    response = get_consent(borrower['mobileNumber'], str(userid))
    consent_id = response['id']

    db["borrowers"].update_one(
        {"_id": userid},
        {"$set": {"consentId": consent_id}}
    )

    return {'consentUrl': response['url']}


@app.get('/all')
def get_all_application():
    borrowers = db["borrowers"].find()
    borrowers_list = []

    for borrower in borrowers:
        obj = {
            'userId': str(borrower['_id']),
            'name': borrower['name'],
            'mobileNumber': borrower['mobileNumber'],
            'applicationDate': borrower['_id'].generation_time,
            'applicationStatus': borrower['applicationStatus']
        }
        borrowers_list.append(obj)

    return borrowers_list


@app.get('/getdatasession')
def get_data_session(userid: str):
    borrower = db['borrowers'].find_one({'_id': ObjectId(userid)})
    consent_id = borrower['consentId']

    time.sleep(5)
    session_id = create_data_session(consent_id)

    db["borrowers"].update_one(
        {"_id": ObjectId(userid)},
        {"$set": {"sessionId": session_id}}
    )

    time.sleep(30)
    res = fetch_and_save_session_data(userid, session_id)

    if res['status'] != "COMPLETED":
        time.sleep(10)
        res = fetch_and_save_session_data(userid, session_id)

    return {"msg": "Successfully fetched and saved data"}


@app.get('/userinfo')
def fetch_user_info(userid: str):
    borrower = db['borrowers'].find_one(
        {'_id': ObjectId(userid)},
        {'_id': False, 'consentId': False, 'sessionId': False}
    )

    financial_details = db['financialData'].find_one({'userid': userid})

    # Handle case where financial data not yet available
    if not financial_details:
        return {
            'error': 'Financial data not yet available. Please complete the Setu AA consent flow first.',
            'status': 'pending'
        }

    account_details = financial_details['accountDetails']
    profile_details = account_details.get('profile', {})

    pincode = borrower['businessPinCode']
    type_of_business = borrower['typeOfBusiness']
    state = borrower['businessState']
    district = borrower['businessDistrict']
    amount_applied = borrower['amountApplied']

    competitor_result = competitor_analysis(pincode, type_of_business)
    opportunity_result = oppurtunity_rating(state, district)
    sectoral_result = sectoral_analysis(type_of_business.lower())
    prosperity_result = relative_prosperity(state, district)
    ease_result = ease_of_business(pincode, state)
    transaction_result = transaction_analysis(userid)

    score = (
        competitor_result['rating'] +
        opportunity_result['rating'] +
        sectoral_result['rating'] +
        prosperity_result['rating'] +
        ease_result['rating']
    )

    allowed_credit = int((score / 500) * amount_applied)

    account_number = account_details.get('maskedAccNumber', 'NA')
    account_type = account_details.get('type', 'NA')

    try:
        holder = profile_details['holders']['holder'][0]
        account_email = holder.get('email', 'NA')
        pan = holder.get('pan', 'NA')
    except Exception:
        account_email = 'NA'
        pan = 'NA'

    return {
        'userFormSubmittedInfo': {
            'Name': borrower['name'],
            'Phone': borrower['mobileNumber'],
            'Business Type': borrower['typeOfBusiness'],
            'Business Adress': borrower['businessAddress'],
            'Amount Applied': borrower['amountApplied']
        },
        'score': round(score, 2),
        'allowedCredit': allowed_credit,
        'accountDetails': {
            'Account Number': account_number,
            'Account Type': account_type,
            'Account Email': account_email,
            'PAN': pan
        },
        'indicators': [
            competitor_result,
            opportunity_result,
            sectoral_result,
            prosperity_result,
            ease_result,
        ],
        'transactionalAnalysis': transaction_result
    }


@app.get('/updateuser')
def update_user(userid: str = None, updateType: str = None, approvedAmount: Optional[int] = 0):
    status_map = {
        'new': ApplicationStatus.newApplication.value,
        'accept': ApplicationStatus.accepted.value,
        'reject': ApplicationStatus.rejected.value,
        'halt': ApplicationStatus.halted.value
    }

    application_status_value = status_map.get(updateType)

    if application_status_value is None:
        return {'msg': f'Invalid updateType: {updateType}'}

    db["borrowers"].update_one(
        {"_id": ObjectId(userid)},
        {"$set": {
            "applicationStatus": application_status_value,
            "approvedAmount": approvedAmount
        }}
    )

    return {'msg': f'Application status updated to {updateType}'}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)


@app.get('/mock/addfakedata')
def add_fake_financial_data(userid: str):
    """
    Mock endpoint to simulate Setu AA financial data.
    Only for testing purposes — remove in production.
    """
    fake_data = {
        'userid': userid,
        'accountDetails': {
            'maskedAccNumber': 'XXXXXXXX9950',
            'type': 'SAVINGS',
            'profile': {
                'holders': {
                    'holder': [
                        {
                            'email': 'testuser@gmail.com',
                            'pan': 'ABCDE1234F',
                            'name': 'Test User',
                            'mobile': '9876543210'
                        }
                    ]
                }
            },
            'summary': {
                'currentBalance': '150000.00',
                'currency': 'INR'
            },
            'transactions': {
                'Transaction': [
                    {
                        'amount': '50000.00',
                        'type': 'CREDIT',
                        'narration': 'Salary credit',
                        'date': '2024-01-01'
                    },
                    {
                        'amount': '15000.00',
                        'type': 'DEBIT',
                        'narration': 'Online shopping payment',
                        'date': '2024-01-05'
                    },
                    {
                        'amount': '5000.00',
                        'type': 'DEBIT',
                        'narration': 'ATM cash withdrawal',
                        'date': '2024-01-10'
                    },
                    {
                        'amount': '45000.00',
                        'type': 'CREDIT',
                        'narration': 'Business income',
                        'date': '2024-01-15'
                    },
                    {
                        'amount': '10000.00',
                        'type': 'DEBIT',
                        'narration': 'Online transfer payment',
                        'date': '2024-01-20'
                    },
                    {
                        'amount': '3000.00',
                        'type': 'DEBIT',
                        'narration': 'ATM cash withdrawal',
                        'date': '2024-01-25'
                    }
                ]
            }
        }
    }

    # Remove existing fake data for this user if any
    db['financialData'].delete_one({'userid': userid})

    # Insert fresh fake data
    result = db['financialData'].insert_one(fake_data)

    return {
        'msg': 'Fake financial data added successfully',
        'userid': userid,
        'inserted_id': str(result.inserted_id)
    }
