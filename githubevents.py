import requests
import pandas
from tqdm import tqdm

# query GitHub Events API and retrieve events for a contributor
def __get_contributor_events(contributor, headers):
    events = []
    for page in range(1, 4):  # Pages 1 to 3
        url = f'https://api.github.com/users/{contributor}/events?per_page=100&page={page}'
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            json_response = response.json()
            if not json_response:
                break
            events.extend(json_response)
        else:
            print(f"Failed to retrieve events for {contributor}. Status code: {response.status_code}")
            break
    return events

def __create_events_dataframe(events):
    data = []
    for event in events:
        event_id = event['id']
        contributor = event['actor']['login']
        repo_name = event['repo']['name']
        event_type = event['type']
        created_at = event['created_at']
        data.append([event_id, contributor, repo_name, event_type, created_at])
    df = pandas.DataFrame(data, columns=['event_id', 'contributor', 'repo_name', 'event_type', 'created_at'])
    df['created_at'] = pd.to_datetime(df.created_at, errors='coerce', format='%Y-%m-%dT%H:%M:%SZ')
    df = df.sort_values('created_at')
    return df

# call this function
def get_events(contributors, headers):
    all_events = []
    for contributor in tqdm(contributors):
        events = __get_contributor_events(contributor, headers)
        all_events.extend(events)

    events_df = __create_events_dataframe(all_events)
    return events_df