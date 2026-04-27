import feedparser
import pandas as pd
from datetime import datetime
import re
import os
import unicodedata
import argparse # Import argparse for command-line arguments

# -----------------------------
# CONFIG
# -----------------------------
RSS_FEEDS = {
    "Avenue de l'IA" : "https://avenuedelia.com/fr/actu/rss/",
    "404 Media" : "https://www.404media.co/rss",
    "Ahead of AI" : "https://magazine.sebastianraschka.com/feed",
    "One Useful Thing" : "https://www.oneusefulthing.org/feed"
}

# OUTPUT_FILE will now be set via command-line argument

# -----------------------------
# FONCTIONS
# -----------------------------

def clean_text(text):
    text = re.sub('<.*?>', '', text)  # remove HTML tags
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8') # Normalize and remove non-ascii characters
    text = re.sub(r'[^\w\s.,:;!?\-\'"\/]', '', text) # Keep alphanumeric, spaces, and common punctuation
    return text.strip()

def detect_theme(text):
    text = text.lower()

    if any(word in text for word in ["ai", "machine learning", "llm", "gpt"]):
        return "IA"
    elif any(word in text for word in ["cloud", "aws", "azure"]):
        return "Cloud"
    elif any(word in text for word in ["data", "analytics", "bi"]):
        return "Data"
    elif any(word in text for word in ["cyber", "security", "hack"]):
        return "Cyber"
    else:
        return "Autre"

def compute_score(date):
    days_old = (datetime.now() - date).days
    max_days_for_score = 30
    score = max(0, 100 - (days_old * (100 / max_days_for_score)))
    return score


# -----------------------------
# MAIN SCRIPT EXECUTION
# -----------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Enrich a CSV file with articles from RSS feeds.')
    parser.add_argument('output_file', type=str, help='The path to the CSV file to enrich.')
    args = parser.parse_args()

    OUTPUT_FILE = args.output_file

    articles = []

    for source, url in RSS_FEEDS.items():
        feed = feedparser.parse(url)

        for entry in feed.entries:
            try:
                published = datetime(*entry.published_parsed[:6])
            except:
                published = datetime.now()

            title = clean_text(entry.title)
            summary = clean_text(entry.summary if "summary" in entry else "")
            link = entry.link

            theme = detect_theme(title + " " + summary)

            articles.append({
                "date": published,
                "source": source,
                "title": title,
                "summary": summary,
                "url": link,
                "theme": theme
            })

    # -----------------------------
    # DATAFRAME PROCESSING AND MERGE
    # -----------------------------

    final_output_columns = ['id', 'date', 'titre', 'url', 'source_nom', 'theme']

    new_articles_df = pd.DataFrame(articles)

    new_articles_df = new_articles_df.rename(columns={
        'source': 'source_nom',
        'title': 'titre',
    })

    new_articles_df['id'] = pd.NA

    new_articles_df = new_articles_df.drop(columns=['summary'])

    new_articles_df['date'] = pd.to_datetime(new_articles_df['date']).dt.strftime('%d/%m/%Y')

    for col in final_output_columns:
        if col not in new_articles_df.columns:
            new_articles_df[col] = pd.NA
    new_articles_df = new_articles_df[final_output_columns]

    existing_df = pd.DataFrame(columns=final_output_columns)
    if os.path.exists(OUTPUT_FILE):
        try:
            temp_df = pd.read_csv(OUTPUT_FILE, sep=';')
            for col in final_output_columns:
                if col not in temp_df.columns:
                    temp_df[col] = pd.NA
            existing_df = temp_df[final_output_columns].copy()

            print(f"Loaded {len(existing_df)} existing articles from {OUTPUT_FILE}")
        except pd.errors.EmptyDataError:
            print(f"Existing CSV file '{OUTPUT_FILE}' is empty. Starting with an empty DataFrame for existing_df.")
        except Exception as e:
            print(f"Error loading existing CSV: {e}. Starting with an empty DataFrame for existing_df. (Details: {e})")
    else:
        print(f"No existing CSV file found at '{OUTPUT_FILE}'. Creating a new one.")

    combined_df = pd.concat([existing_df, new_articles_df], ignore_index=True)

    combined_df = combined_df.drop_duplicates(subset=['url'], keep='first')

    combined_df = combined_df.reset_index(drop=True)
    combined_df['id'] = (combined_df.index + 1).astype(int)

    combined_df = combined_df.sort_values(by="id", ascending=False)

    combined_df.to_csv(OUTPUT_FILE, index=False, sep=';')

    print(f"{len(combined_df)} articles sauvegardés dans {OUTPUT_FILE}")
    # Display head for verification - replaced display() with print() for terminal use
    print(combined_df.head().to_string())