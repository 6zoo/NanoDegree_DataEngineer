import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def missing_value_ratio(df):
    """Visualize missing values in the dataFrame for EDA purpose
    :param df:
    :return barplots:
    """

    miss_df = pd.DataFrame(data= df.isnull().sum(), columns=['values'])
    miss_df = miss_df.reset_index()
    miss_df.columns = ['cols', 'values']
    
    # get ratio of the missing values along with its entire rows and visualize
    miss_df['% of missing values'] = miss_df['values']/df.shape[0] * 100

    plt.rcdefaults()
    plt.figure(figsize=(10,5))

    ax = sns.barplot(x="cols", y="% of missing values", data=miss_df)
    ax.set_title(df.name)
    ax.set_ylim(0, 100)
    ax.set_xticklabels(ax.get_xticklabels(), rotation=90)

    plt.show()
    
    
def drop_duplicates_and_remeasure(df):
    """Given a dataframe df, get duplicated rows and drop them and re-measure whether any duplicates are left
    :param df:
    :return:
    """
    duplicates_df = df
    
    dup_rows = duplicates_df.duplicated().sum()
    print("duplicated rows count : ", dup_rows)
    duplicates_df = duplicates_df.drop_duplicates()
    dup_rows = duplicates_df.duplicated().sum()
    print("duplicated rows count after drops : ", dup_rows)
