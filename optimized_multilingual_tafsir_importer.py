import requests
import json

# Function to fetch translations and tafsirs simultaneously

def fetch_multilingual_data(language, translation_ids, tafsir_id):
    translations = []
    tafsir_data = None
    
    # Fetch translations
    for translation_id in translation_ids:
        response = requests.get(f'https://api.example.com/translations/{translation_id}?lang={language}')
        if response.status_code == 200:
            translations.append(response.json())
        else:
            print(f'Error fetching translation ID {translation_id} for language {language}')

    # Fetch tafsir
    tafsir_response = requests.get(f'https://api.example.com/tafirs/{tafsir_id}?lang={language}')
    if tafsir_response.status_code == 200:
        tafsir_data = tafsir_response.json()
    else:
        print(f'Error fetching tafsir ID {tafsir_id} for language {language}')

    # Combine translations and tafsir
    combined_data = {'translations': translations, 'tafsir': tafsir_data}
    return combined_data

# Example usage
if __name__ == '__main__':
    # Define languages, translation IDs, and tafsir ID
    languages = {'ar': [1, 2], 'en': [1, 3]}
    tafsir_id = 1
    
    for lang, ids in languages.items():
        data = fetch_multilingual_data(lang, ids, tafsir_id)
        print(json.dumps(data, indent=2))
