"""
@File name: map_plot
@Andrew IDS: yangyond, hhe3, mfouad, ziruiw2
@Purpose: plot a map of every state in U.S. based on university counts.
"""

import pandas as pd
import os
import plotly.express as px
import plotly.graph_objects as go


def map_plot(df_raw):
    df_raw = df_raw.dropna(subset=['City', 'State'], how='any')

    state_count = df_raw['State'].value_counts().reset_index()
    state_count.columns = ['State', 'Count']
    df = state_count
    state_abbr = {
        'California': 'CA',
        'New York': 'NY',
        'Arizona': 'AZ',
        'Nebraska': 'NE',
        'Colorado': 'CO',
        'Utah': 'UT',
        'Ohio': 'OH',
        'Connecticut': 'CT',
        'Minnesota': 'MN',
        'Florida': 'FL',
        'Wisconsin': 'WI',
        'Georgia': 'GA',
        'Pennsylvania': 'PA',
        'Michigan': 'MI',
        'North Carolina': 'NC',
        'Indiana': 'IN',
        'New Jersey': 'NJ',
        'Massachusetts': 'MA',
        'Maryland': 'MD',
        'Illinois': 'IL',
        'Texas': 'TX',
        'Tennessee': 'TN'
    }
    df['State'] = df['State'].map(state_abbr)

    fig = go.Figure(data=go.Choropleth(
        locations=df['State'],  # using abbreviations
        z=df['Count'].astype(float),  # Data to be color-coded
        locationmode='USA-states',  # set of locations match entries in `locations`
        colorscale='Reds',
        colorbar_title="Count",
    ))

    fig.update_layout(
        title_text='CS Program Distribution Heatmap',
        geo_scope='usa',
    )

    fig.show()


if __name__ == "__main__":
    df_raw = pd.read_csv(os.path.join('merge_previous', 'merged.csv'))
    map_plot(df_raw)
