from operator import itemgetter
import pandas as pd
import requests
import os
from dotenv import load_dotenv

load_dotenv()

GEOAPIFY_API_KEY = os.getenv('GEOAPIFY_API_KEY')

# All Indian states list for ease_of_business calculation
ALL_STATES_LIST = [
    'andaman-&-nicobar-islands', 'andhra-pradesh', 'arunachal-pradesh',
    'assam', 'bihar', 'chandigarh', 'chhattisgarh',
    'dadra-&-nagar-haveli-&-daman-&-diu', 'delhi', 'goa', 'gujarat',
    'haryana', 'himachal-pradesh', 'jammu-&-kashmir', 'jharkhand',
    'karnataka', 'kerala', 'ladakh', 'lakshadweep', 'madhya-pradesh',
    'maharashtra', 'manipur', 'meghalaya', 'mizoram', 'nagaland',
    'odisha', 'puducherry', 'punjab', 'rajasthan', 'sikkim',
    'tamil-nadu', 'telangana', 'tripura', 'uttar-pradesh',
    'uttarakhand', 'west-bengal'
]

# Hardcoded sector ratings based on market research
SECTOR_BOOM_RATINGS = {
    'fashion': 40,
    'hospitality': 51,
    'jewellery': 87,
    'entertainment': 67,
    'daily-essentials': 91
}


def competitor_analysis(pincode, typeOfBusiness):
    try:
        # Step 1 - resolve pincode to lat/lon
        resolve_pincode_url = (
            f"https://api.geoapify.com/v1/geocode/autocomplete"
            f"?text={pincode}&apiKey={GEOAPIFY_API_KEY}&limit=1"
        )
        res1 = requests.get(resolve_pincode_url, timeout=10).json()

        if not res1.get('features'):
            raise ValueError("Pincode not resolved")

        properties = res1['features'][0]['properties']
        lon = properties['lon']
        lat = properties['lat']

        # Step 2 - find competitors nearby within 5km
        place_api_url = (
            f"https://api.geoapify.com/v2/places"
            f"?categories=commercial.supermarket"
            f"&filter=circle:{lon},{lat},5000"
            f"&limit=20"
            f"&apiKey={GEOAPIFY_API_KEY}"
        )
        res2 = requests.get(place_api_url, timeout=10).json()
        competitors_obj = res2.get('features', [])

        competitor_list = []
        for comp in competitors_obj:
            name = comp.get('properties', {}).get('name')
            if name:
                competitor_list.append(name)

        number_of_competitors = len(competitor_list)

        # Avoid division by zero
        if number_of_competitors == 0:
            competitor_rating = 100.0
        else:
            competitor_rating = round(100 / number_of_competitors, 2)
            # Cap at 100
            competitor_rating = min(competitor_rating, 100.0)

        return {
            'name': 'Competition Score',
            'rating': competitor_rating,
            'competitors': competitor_list,
            'remarks': ''
        }

    except Exception as e:
        print(f"competitor_analysis error: {e}")
        return {
            'name': 'Competition Score',
            'rating': 50.0,
            'competitors': [],
            'remarks': 'Could not fetch competitor data'
        }


def oppurtunity_rating(state, businessDistrict):
    try:
        # Use latest 2024 Q4 data
        data = pd.read_json(
            f"pulsedata/map/user/hover/country/india/state/{state}/2024/4.json"
        )
        hoverdata = data['data']['hoverData']

        app_open_ratio_list = []
        for district_key in hoverdata:
            district_data = hoverdata[district_key]
            registered = district_data.get('registeredUsers', 0)
            app_opens = district_data.get('appOpens', 0)

            if registered > 0:
                ratio = app_opens / registered
            else:
                ratio = 0

            # Clean district name
            district_name = district_key
            if 'district' in district_key.lower():
                idx = district_key.lower().find('district')
                district_name = district_key[:idx].strip()

            app_open_ratio_list.append({
                'district': district_name.lower(),
                'ratio': ratio
            })

        sorted_list = sorted(app_open_ratio_list, key=lambda i: i['ratio'])

        district_index = next(
            (index for (index, d) in enumerate(sorted_list)
             if d["district"] == businessDistrict.lower()),
            None
        )

        if district_index is None:
            opportunity_score = 50.0
        else:
            opportunity_score = round((1 - (district_index / len(sorted_list))) * 100, 2)

        return {
            'name': 'Opportunity Score',
            'rating': opportunity_score
        }

    except Exception as e:
        print(f"oppurtunity_rating error: {e}")
        return {
            'name': 'Opportunity Score',
            'rating': 50.0
        }


def sectoral_analysis(typeOfBusiness):
    try:
        business_type = typeOfBusiness.lower().strip()
        rating = SECTOR_BOOM_RATINGS.get(business_type, 50)

        # Build sector comparison list
        sorted_sectors = sorted(
            SECTOR_BOOM_RATINGS.items(),
            key=lambda x: x[1],
            reverse=True
        )
        sectors_dict = {k: v for k, v in sorted_sectors}

        return {
            'name': 'Sectoral Score',
            'rating': rating,
            'sectors': sectors_dict,
            'remark': ''
        }

    except Exception as e:
        print(f"sectoral_analysis error: {e}")
        return {
            'name': 'Sectoral Score',
            'rating': 50,
            'sectors': SECTOR_BOOM_RATINGS,
            'remark': ''
        }


def relative_prosperity(state, district):
    try:
        # Use latest 2024 Q4 data
        data = pd.read_json(
            f'pulsedata/map/transaction/hover/country/india/state/{state}/2024/4.json'
        )
        hoverdata_list = data['data']['hoverDataList']

        district_wise_list = []
        for elem in hoverdata_list:
            name = elem.get('name', '').lower()
            metric = elem.get('metric', [])
            if metric:
                amount = metric[0].get('amount', 0)
                district_wise_list.append({
                    'districtName': name,
                    'amount': amount
                })

        if not district_wise_list:
            raise ValueError("No district data found")

        # Sort ascending by amount
        sorted_by_amount = sorted(district_wise_list, key=itemgetter('amount'))

        # Find district rank
        district_score = 0
        for i, dist in enumerate(sorted_by_amount):
            if dist['districtName'] == district.lower() or dist['districtName'] == district.lower() + ' district':
                district_score = i
                break

        prosperity_rating = round(
            (district_score / len(district_wise_list)) * 100, 2
        )

        # Get top 3 most prosperous districts
        top_3_districts = [
            d['districtName'] for d in sorted_by_amount[-3:][::-1]
        ]

        return {
            'name': 'Prosperity Score',
            'rating': prosperity_rating,
            'moreProsperousAreas': top_3_districts,
            'remark': ''
        }

    except Exception as e:
        print(f"relative_prosperity error: {e}")
        return {
            'name': 'Prosperity Score',
            'rating': 50.0,
            'moreProsperousAreas': [],
            'remark': 'Could not fetch prosperity data'
        }


def ease_of_business(pincode, state):
    try:
        all_state_merchant_payment = []

        # Use latest 2024 Q4 data for all states
        for state_name in ALL_STATES_LIST:
            try:
                state_data = pd.read_json(
                    f"pulsedata/aggregated/transaction/country/india/state/{state_name}/2024/4.json"
                )
                transaction_list = state_data['data']['transactionData']
                for transaction in transaction_list:
                    if transaction['name'] == 'Merchant payments':
                        amount = transaction['paymentInstruments'][0]['amount']
                        all_state_merchant_payment.append({
                            'stateName': state_name,
                            'merchant_amount': amount
                        })
                        break
            except Exception:
                continue

        if not all_state_merchant_payment:
            raise ValueError("No merchant payment data found")

        # Load population data
        population_data = pd.read_json(
            "otherdata/state-wise-population.json",
            typ='series'
        )

        # Calculate per capita merchant payment
        all_state_per_capita = []
        for item in all_state_merchant_payment:
            state_name = item['stateName']
            try:
                population = population_data[state_name]
                if population > 0:
                    per_capita = item['merchant_amount'] / population
                else:
                    per_capita = 0
            except Exception:
                per_capita = 0

            all_state_per_capita.append({
                'stateName': state_name,
                'perCapita': per_capita
            })

        # Sort descending by per capita
        sorted_per_capita = sorted(
            all_state_per_capita,
            key=itemgetter('perCapita'),
            reverse=True
        )

        # Find rank of input state
        input_state_rank = 0
        for i, elem in enumerate(sorted_per_capita):
            if elem['stateName'] == state.lower():
                input_state_rank = i
                break

        total_states = len(sorted_per_capita)
        ease_rating = round(
            (1 - (input_state_rank / total_states)) * 100, 2
        )

        # Get top 3 states
        top_3_states = [s['stateName'] for s in sorted_per_capita[:3]]

        return {
            'name': 'Ease of Business Score',
            'rating': ease_rating,
            'betterAreas': top_3_states,
            'remark': ''
        }

    except Exception as e:
        print(f"ease_of_business error: {e}")
        return {
            'name': 'Ease of Business Score',
            'rating': 50.0,
            'betterAreas': [],
            'remark': 'Could not fetch ease of business data'
        }
