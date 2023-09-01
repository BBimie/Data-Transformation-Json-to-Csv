# DATA TRANSFORMMATION JSON to CSV

Project was provided by [Stratascratch](https://platform.stratascratch.com/data-projects/data-transformation)

### Problem Statement:
Chama is a relatively new, modern company, with an IOS App and Android App. Some information about App usage is generated by the App and backend API. Other sources of information, like the Google Play Store, provide very useful insights on App performance and user rating.

Some event data come as json files and need some transformation to be structured as tables. Convert the case.json file to 3 csv files, with the following guideline.

1. `CuratedOfferOptions.csv`
    ``` CurationProvider: in quotes
        OfferId: in quotes
        DealerId: in quotes
        UniqueOptionId: in quotes
        OptionId: in quotes
        IsMobileDealer: without quotes
        IsOpen: without quotes
        Eta: in quotes
        ChamaScore: without quotes
        ProductBrand: in quotes
        IsWinner: without quotes
        MinimumPrice: without quotes
        MaximumPrice: without quotes
        DynamicPrice: without quotes
        FinalPrice: without quotes
        DefeatPrimaryReason: in quotes
        DefeatReasons: in quotes
        EnqueuedTimeSP: DD/MM/YYYY (converted to Brasilian timezone - UTC-3) ```

2. `DynamicPriceOption.csv`
    ``` Provider: in quotes
        OfferId: in quotes
        UniqueOptionId: in quotes
        BestPrice: without quotes
        EnqueuedTimeSP: DD/MM/YYYY (converted to Brasilian timezone - UTC-3) ```

3. `DynamicPriceRange.csv`:
    ``` Provider: in quotes
        OfferId: in quotes
        MinGlobal: without quotes
        MinRecommended: without quotes
        MaxRecommended: without quotes
        DifferenceMinRecommendMinTheory: without quotes
        EnqueuedTimeSP: DD/MM/YYYY (converted to Brasilian timezone - UTC-3) ```


### Understanding the data

The case.json file contains a collection of `DynamicPrice_Result` and `CurateOffer_Result` events, the event metadata is stored in `payload` and can have different structure based on the `provider` or `curationProvider` type.
The resulting csv file names are derived from the `provider` or `curationProvider` so it is easy to identify which event should go into each file.

### Transformation
I created 3 separate methods to extract the different events because I wanted to be clear and precise and easy to read, I extracted the neccessary data from the json file using different python methods and modules, and stored the data in their respective csv files using pandas.

I did not need to carry out and datatype conversion except for the `EnqueuedTimeUtc` object that was converted to Brazilian time (UTC3)


### To replicate this
- Clone this repo
- Install python
- Install Pandas
- run `python -m main.py`
