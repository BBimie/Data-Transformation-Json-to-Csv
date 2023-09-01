import json
import pandas as pd

#open the json file
with open('data/raw/case.json') as json_file:
        json_data = json.load(json_file)


def curated_offer_options(json_data)-> pd.DataFrame:
    """ This function extracts curated offer events """
    data = []
    for item in json_data:
        
        if 'curationProvider' in item['Payload']:
            payload = json.loads(item['Payload'])
            
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
                          'DefeatPrimaryReason': (option.get('defeatPrimaryReason', "")),
                          'DefeatReasons': ', '.join(option.get('defeatReasons', [])),
                          'EnqueuedTimeSP' : pd.Timestamp(item['EnqueuedTimeUtc']).tz_convert('America/Sao_Paulo').strftime('%d/%m/%Y')
                          }
                    data.append(row)
    return pd.DataFrame(data)


def dynamic_price_option(json_data) -> pd.DataFrame:
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


curated_offer_options().to_csv('data/processed/CuratedOfferOptions.csv', index=False)
dynamic_price_option().to_csv('data/processed/DynamicPriceOption.csv', index=False)
dynamic_price_range().to_csv('data/processed/DynamicPriceRange.csv', index=False)