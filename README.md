ID2223 project

Alexander Dahm & Filip Stenbom

The goal of this project is to predict electricity prices in Sweden using weather data. The analysis focuses on electricity price zone SE2, which has a high share of wind and hydropower generation.

Weather data is collected from five stations within SE2. The stations are selected based on either:
- Proximity to major population centers with high electricity consumption, or
- Proximity to major power generation facilities

This selection aims to capture weather conditions that are most relevant for both electricity supply and demand in the region.

The weather data is provided by openmeteo (same as lab 1), and energy data is gathered from nordpool. Due to us not being able to obtain free API access to nordpool we are entering the energyprices manually into a dict every day.

We are using hopsworks in the same ways as in Lab 1.

