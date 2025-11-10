import pandas as pd
import os
import re

# --- Konfiguracja plików ---
OUTPUT_FILE = '01_Combined_and_Deduplicated_Data.csv'
CLEANED_OUTPUT_FILE = '02_Cleaned_Text_for_Analysis.csv'

def clean_text(text):
    """
    Wykonuje podstawowe czyszczenie tekstu:
    1. Konwertuje do małych liter.
    2. Usuwa linki (URL).
    3. Usuwa wzmianki (@username) i hasztagi (#tag).
    4. Usuwa znaki interpunkcyjne (oprócz standardowych spacji).
    5. Usuwa liczby.
    """
    if not isinstance(text, str):
        return ""
    
    # 1. Małe litery
    text = text.lower()
    
    # 2. Usuwanie linków (URL)
    text = re.sub(r'http\S+|www\S+|https\S+', '', text, flags=re.MULTILINE)
    
    # 3. Usuwanie wzmianek (@username) i hasztagów (#tag)
    text = re.sub(r'@\w+|#\w+', '', text)
    
    # 4. Usuwanie znaków specjalnych, interpunkcyjnych i emotikonów, zachowując litery i spacje
    # Używamy zakresu dla polskich znaków diakrytycznych: a-z, kropki (.).
    # Lepiej usuwać większość, by nie zaśmiecać analizy sentymentu.
    text = re.sub(r'[^\w\s]', ' ', text)
    
    # 5. Usuwanie cyfr
    text = re.sub(r'\d+', '', text)
    
    # 6. Usuwanie dodatkowych spacji i przycinanie
    text = re.sub(r'\s+', ' ', text).strip()
    
    return text

def process_data_pipeline():
    """Główna funkcja wykonująca łączenie, deduplikację i czyszczenie."""
    print("--- START: Przygotowanie danych do analizy politycznej ---")
    
    # 1. Łączenie wszystkich plików CSV
    csv_files = [f for f in os.listdir('.') if f.endswith('.csv') and f not in [OUTPUT_FILE, CLEANED_OUTPUT_FILE]]
    
    if not csv_files:
        print("Brak plików CSV do przetworzenia. Upewnij się, że pliki znajdują się w tym samym folderze, co skrypt.")
        return

    all_data = []
    total_rows_before_deduplication = 0
    
    for file in csv_files:
        try:
            df = pd.read_csv(file)
            all_data.append(df)
            total_rows_before_deduplication += len(df)
        except Exception as e:
            print(f"[BŁĄD] Nie udało się wczytać pliku {file}: {e}")

    if not all_data:
        print("Nie udało się wczytać żadnego pliku. Przerywam.")
        return

    df_combined = pd.concat(all_data, ignore_index=True)

    print(f"1. POŁĄCZENIE: Wczytano {len(csv_files)} plików. Całkowita liczba wierszy: {total_rows_before_deduplication}")
    
    # 2. Usuwanie duplikatów na podstawie treści
    if 'text' not in df_combined.columns:
        print("[BŁĄD] W wczytanych danych brakuje kluczowej kolumny 'text'. Przerywam.")
        return

    # Wypełnienie NaN pustymi stringami dla bezpieczeństwa
    df_combined['text'] = df_combined['text'].astype(str).fillna('')
    
    df_deduplicated = df_combined.drop_duplicates(subset=['text'], keep='first')
    
    removed_duplicates = total_rows_before_deduplication - len(df_deduplicated)
    
    print(f"2. DEDUPILKACJA: Po usunięciu duplikatów po treści pozostało {len(df_deduplicated)} unikalnych wpisów.")
    print(f"   -> Usunięto duplikatów: {removed_duplicates}")
    
    # Zapisanie zbioru po deduplikacji (dla celów kontrolnych)
    df_deduplicated.to_csv(OUTPUT_FILE, index=False)
    print(f"   -> Zapisano zbiór po deduplikacji do: {OUTPUT_FILE}")

    # 3. Czyszczenie tekstu
    print("3. CZYSZCZENIE: Wykonywanie zaawansowanego czyszczenia tekstu...")
    # Utworzenie nowej kolumny z czystym tekstem
    df_deduplicated['cleaned_text'] = df_deduplicated['text'].apply(clean_text)
    
    # Opcjonalnie: usunięcie wierszy, które po czyszczeniu mają pusty tekst (np. były tylko hasztagami)
    initial_clean_rows = len(df_deduplicated)
    df_final = df_deduplicated[df_deduplicated['cleaned_text'] != ''].reset_index(drop=True)
    rows_dropped_after_clean = initial_clean_rows - len(df_final)

    print(f"   -> Usunięto wierszy, których treść była pusta po czyszczeniu: {rows_dropped_after_clean}")
    
    # 4. Zapisanie ostatecznego zbioru
    df_final.to_csv(CLEANED_OUTPUT_FILE, index=False)
    
    print(f"\n--- KONIEC: Proces zakończony pomyślnie ---")
    print(f"Ostateczny zbiór danych gotowy do analizy (kolumna 'cleaned_text') zapisano do pliku: {CLEANED_OUTPUT_FILE}")
    print(f"Liczba wpisów w finalnym zbiorze: {len(df_final)}")
    
if __name__ == "__main__":
    process_data_pipeline()