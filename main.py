import json
import pandas as pd

#open the json file
print('Read Json file')
with open('data/raw/case.json') as json_file:
        json_data = json.load(json_file)
        


def curated_offer_options(json_data)-> pd.DataFrame:
    """ This function extracts curated offer events """

    print('Extract Curated offer options data')

    data = []
    for item in json_data:
        #check for event curationProvider, we are using 'in' because the data is formatted as a string at this point
        if 'curationProvider' in item['Payload']:
            #convert data to json/dict
            payload = json.loads(item['Payload'])
            
            #loop through payload and then options to get to the base level data
            for curation_provider in payload:
                for option in curation_provider['options']:
                    row = { 'OfferId' : curation_provider['offerId'],
                          'DealerId' : curation_provider['dealerId'],
                          'UniqueOptionId' : option['uniqueOptionId'],
                          'OptionId' : option['optionId'],
                          'IsMobileDealer' : option['isMobileDealer'],
                          'IsOpen' : option['isOpen'],
                          'ETA' : option['eta'],
                          'ChamaScore' : option['chamaScore'],
                          'ProductBrand' : option['productBrand'],
                          'IsWinner' : option['isWinner'],
                          'MinimumPrice' : option['minimumPrice'],
                          'MaximumPrice' : option['maximumPrice'],
                          'DynamicPrice' : option['dynamicPrice'],
                          'FinalPrice' : option['finalPrice'],
                          #return value if present
                          'DefeatPrimaryReason': (option.get('defeatPrimaryReason', "")),
                          'DefeatReasons': ', '.join(option.get('defeatReasons', [])),
                          'EnqueuedTimeSP' : pd.Timestamp(item['EnqueuedTimeUtc']).tz_convert('America/Sao_Paulo').strftime('%d/%m/%Y')
                          }
                    data.append(row)
    return pd.DataFrame(data)


def dynamic_price_option(json_data) -> pd.DataFrame:
    """ This function extracts dynamic price option events """

    print('Extract dynamic price option data')
    
    data = []
        
    for item in json_data:
        #extract data needed from item payload
        if 'ApplyDynamicPricePerOption' in item['Payload']:
            payload = json.loads(item['Payload'])
            for algoOut in payload['algorithmOutput']:
                row = {'Provider' : payload['provider'],
                      'OfferId' : payload['offerId'],
                      'UniqueOptionId': algoOut['uniqueOptionId'],
                      'BestPrice': algoOut['bestPrice'],
                      'EnqueuedTimeSP' : pd.Timestamp(item['EnqueuedTimeUtc']).tz_convert('America/Sao_Paulo').strftime('%d/%m/%Y')
                }
                data.append(row)
    return pd.DataFrame(data)


def dynamic_price_range(json_data) -> pd.DataFrame:
    """ This function extracts dynamic price range events """

    print('Extract dynamic price range data')
    
    data = []
    for item in json_data:
        #extract data needed from item payload
        if 'ApplyDynamicPriceRange' in item['Payload']:
            payload = json.loads(item['Payload'])

            row = {'Provider': payload['provider'],
                  'OfferId': payload['offerId'],
                  'MinGlobal' : payload['algorithmOutput']['min_global'],
                  'MinRecommended' : payload['algorithmOutput']['min_recommended'],
                  'MaxRecommended' : payload['algorithmOutput']['max_recommended'],
                  'DifferenceMinRecommendMinTheory' : payload['algorithmOutput']['differenceMinRecommendMinTheory'],
                  'EnqueuedTimeSP' : pd.Timestamp(item['EnqueuedTimeUtc']).tz_convert('America/Sao_Paulo').strftime('%d/%m/%Y')
                  }
            data.append(row)
    return pd.DataFrame(data)


curated_offer_options(json_data=json_data).to_csv('data/processed/CuratedOfferOptions.csv', index=False)
dynamic_price_option(json_data=json_data).to_csv('data/processed/DynamicPriceOption.csv', index=False)
dynamic_price_range(json_data=json_data).to_csv('data/processed/DynamicPriceRange.csv', index=False)