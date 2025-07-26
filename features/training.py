import calendar
import dataset
import cacher

# We are training at 12 am Asia time. 
@cacher.load_or_save_dataframe(subdir='model', save_type='pkl', non_empty=False)
def training(symbol, date, look_back_days, frequency):
    # Always pass this date_tm through calendar get_train_freq function
    calendar.get_training_dates(date, lookback_days, frequency= frequency)