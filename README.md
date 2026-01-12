# ID2223 project

Link to dashboard: https://flipperj.github.io/mlfs-book-proj/

### Alexander Dahm & Filip Stenbom

The goal of this project is to predict electricity prices in Sweden using weather data. The analysis focuses on electricity price zone SE2, which has a high share of wind and hydropower generation.

Weather data is collected from five stations within SE2. The stations are selected based on either:
- Proximity to major population centers with high electricity consumption, or
- Proximity to major power generation facilities

This selection aims to capture weather conditions that are most relevant for both electricity supply and demand in the region.

The weather data is provided by openmeteo (same as lab 1), and energy data is gathered from Nordpool. Due to us not being able to obtain free API access to nordpool we are entering the energyprices manually into a dict every day.

Much of the code is re-used fromm lab 1 in ID2223 given during fall-2025. We are also using hopsworks in the same ways as in the aforementioned lab.

## Preview of historic results (More infromation can be found in the dashboard)
<img width="1007" height="684" alt="Skärmavbild 2026-01-12 kl  08 58 44" src="https://github.com/user-attachments/assets/1ab4c398-d79c-44d4-9c59-87058e893dc4" />

<img width="1015" height="675" alt="Skärmavbild 2026-01-12 kl  08 58 56" src="https://github.com/user-attachments/assets/20aa160f-62ba-4b5d-a91f-de505b28e8a4" />


## Future todo:
- Automated webscraping of daily energy price from https://data.nordpoolgroup.com/auction/day-ahead/prices?deliveryDate=latest&currency=SEK&aggregation=DailyAggregate&deliveryAreas=SE2
