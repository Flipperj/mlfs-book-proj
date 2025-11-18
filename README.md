## Grade ‘E’ tasks
1. Write a backfill feature pipeline that downloads historical weather data (ideally >1 year of data), loads a csv file with
historical air quality data (downloaded from https://aqicn.org) and registers them as 2 Feature Groups with Hopsworks.
2. Schedule a daily feature pipeline notebook that downloads yesterday’s weather data and air quality data, and also the
weather prediction for the next 7-10 days and update the Feature Groups in Hopsworks. Use GH Actions or Modal.
3. Write a training pipeline that (1) selects the features for use in a feature view, (2) reads training data with the Feature
View, trains a regression or classifier model to predict air quality (pm25). Register the model with Hopsworks.
4. Write a batch inference pipeline that creates a dashboard. The program should download your model from Hopsworks
and plot a dashboard that predicts the air quality for the next 7-10 days for your chosen air quality sensor.
5. Monitor the accuracy of your predictions by plotting a hindcast graph showing your predictions vs outcomes
(measured air quality).

We have finished all grade E task using the a sensor located in Östersund, radhusgatan.

## Grade ‘C’ tasks  
6. Update your Model by adding a new feature, lagged air quality for the previous 1 day, 2 days, and 3 days. Measure and
explain the performance improvement or regression for these features.

For task C, we added this new feature. We decided to calculate the mean pm2.5 value of the previous 3 days and use that as the feature. Both model's predictions on the same data are shown in the resulting graphs. As for the performance, the first model had a lower R2 value, whilst the new model had a lower MSE. When analysing the results, both models are quite similar, however, the new model consistently predicts a bit lower pm2.5 values. Addionaly, looking at feature importance, the newly added feature was the most important in the new model. The cause of the results are not fully clear, but we hypothesis that this new feature is not perfect, since pm2.5 levels can change drastically and the historic values can therefore hinder the new model in some cases. This becomes evident when the pm2.5 values rise and the new model usually predict a lower value.

## Grade ‘A’ tasks  
7. Provide predictions for all air quality sensors in a Swedish city

For this task we have implemented predictions for all sensors with available data in Jämtland, Sweden. In total there are 5 sensors, and the results of the daily predictions and corresponding hindcast charts can be found here: https://aldahm.github.io/mlfs-book/air-quality/
