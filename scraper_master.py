# scraper_master.py
# Unifica la lógica de tus 9 scrapers en un solo script.
# Al elegir país -> liga, scrapea todas las tablas disponibles para esa liga y las muestra en consola.
# No importa los módulos originales: todo está embebido aquí.

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import time
import os
from datetime import datetime
import sqlite3

# ---------------------------
# CONFIG: Cambia si usas otro chromedriver
# ---------------------------
CHROMEDRIVER_PATH = "chromedriver.exe"  # ajusta si es necesario
PAGE_SLEEP = 2.5  # espera general entre cargas (puedes subir a 3-4 si tu conexión es lenta)

# ---------------------------
# URLS unificadas por liga/país y por estadística
# (usé las URL que venían en tus scrapers)
# Estructura:
# LIGAS_UNIFICADAS[country][league] = { 'partidos_jugados': url, 'xg': url, ... }
# ---------------------------
LIGAS_UNIFICADAS = {
    "España": {
        "La Liga": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/clean_sheet_team/laliga-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/expected_goals_team/laliga-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/expected_goals_conceded_team/laliga-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/touches_in_opp_box_team/laliga-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/total_yel_card_team/laliga-teams",
            "possession":  "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/possession_percentage_team/laliga-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/fk_foul_lost_team/laliga-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/ontarget_scoring_att_team/laliga-teams",
            "corners":     "https://www.fotmob.com/es/leagues/87/stats/season/27233/teams/corner_taken_team/laliga-teams"
        },
        "La Liga 2": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/clean_sheet_team/laliga2-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/expected_goals_team/laliga2-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/expected_goals_conceded_team/laliga2-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/touches_in_opp_box_team/laliga2-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/total_yel_card_team/laliga2-teams",
            "possession":  "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/possession_percentage_team/laliga2-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/fk_foul_lost_team/laliga2-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/ontarget_scoring_att_team/laliga2-teams",
            "corners":     "https://www.fotmob.com/es/leagues/140/stats/season/27234/teams/corner_taken_team/laliga2-teams"
        }
    },
    "Inglaterra": {
        "Premier League": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/clean_sheet_team/premier-league-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/expected_goals_team/premier-league-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/expected_goals_conceded_team/premier-league-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/touches_in_opp_box_team/premier-league-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/total_yel_card_team/premier-league-teams",
            "possession":  "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/possession_percentage_team/premier-league-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/fk_foul_lost_team/premier-league-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/ontarget_scoring_att_team/premier-league-teams",
            "corners":     "https://www.fotmob.com/es/leagues/47/stats/season/27110/teams/corner_taken_team/premier-league-teams"
        },
        "Championship": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/clean_sheet_team/championship-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/expected_goals_team/championship-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/expected_goals_conceded_team/championship-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/touches_in_opp_box_team/championship-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/total_yel_card_team/championship-teams",
            "possession":  "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/possession_percentage_team/championship-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/fk_foul_lost_team/championship-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/ontarget_scoring_att_team/championship-teams",
            "corners":     "https://www.fotmob.com/es/leagues/48/stats/season/27195/teams/corner_taken_team/championship-teams"
        }
    },
    "Alemania": {
        "Bundesliga": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/clean_sheet_team/bundesliga-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/expected_goals_team/bundesliga-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/expected_goals_conceded_team/bundesliga-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/touches_in_opp_box_team/bundesliga-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/total_yel_card_team/bundesliga-teams",
            "possession":  "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/possession_percentage_team/bundesliga-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/fk_foul_lost_team/bundesliga-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/ontarget_scoring_att_team/bundesliga-teams",
            "corners":     "https://www.fotmob.com/es/leagues/54/stats/season/26891/teams/corner_taken_team/bundesliga-teams"
        },
        "Bundesliga 2": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/clean_sheet_team/2-bundesliga-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/expected_goals_team/2-bundesliga-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/expected_goals_conceded_team/2-bundesliga-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/touches_in_opp_box_team/2-bundesliga-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/total_yel_card_team/2-bundesliga-teams",
            "possession":  "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/possession_percentage_team/2-bundesliga-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/fk_foul_lost_team/2-bundesliga-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/ontarget_scoring_att_team/2-bundesliga-teams",
            "corners":     "https://www.fotmob.com/es/leagues/146/stats/season/26892/teams/corner_taken_team/2-bundesliga-teams"
        }
    },
    "Italia": {
        "Serie A": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/clean_sheet_team/serie-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/expected_goals_team/serie-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/expected_goals_conceded_team/serie-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/touches_in_opp_box_team/serie-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/total_yel_card_team/serie-teams",
            "possession":  "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/possession_percentage_team/serie-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/fk_foul_lost_team/serie-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/ontarget_scoring_att_team/serie-teams",
            "corners":     "https://www.fotmob.com/es/leagues/55/stats/season/27044/teams/corner_taken_team/serie-teams"
        },
        "Serie B": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/clean_sheet_team/serie-b-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/expected_goals_team/serie-b-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/expected_goals_conceded_team/serie-b-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/touches_in_opp_box_team/serie-b-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/total_yel_card_team/serie-b-teams",
            "possession":  "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/possession_percentage_team/serie-b-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/fk_foul_lost_team/serie-b-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/ontarget_scoring_att_team/serie-b-teams",
            "corners":     "https://www.fotmob.com/es/leagues/86/stats/season/27577/teams/corner_taken_team/serie-b-teams"
        }
    },
    "Francia": {
        "Ligue 1": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/clean_sheet_team/ligue-1-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/expected_goals_team/ligue-1-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/expected_goals_conceded_team/ligue-1-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/touches_in_opp_box_team/ligue-1-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/total_yel_card_team/ligue-1-teams",
            "possession":  "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/possession_percentage_team/ligue-1-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/fk_foul_lost_team/ligue-1-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/ontarget_scoring_att_team/ligue-1-teams",
            "corners":     "https://www.fotmob.com/es/leagues/53/stats/season/27212/teams/corner_taken_team/ligue-1-teams"
        },
        "Ligue 2": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/clean_sheet_team/ligue-2-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/expected_goals_team/ligue-2-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/expected_goals_conceded_team/ligue-2-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/touches_in_opp_box_team/ligue-2-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/total_yel_card_team/ligue-2-teams",
            "possession":  "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/possession_percentage_team/ligue-2-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/fk_foul_lost_team/ligue-2-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/ontarget_scoring_att_team/ligue-2-teams",
            "corners":     "https://www.fotmob.com/es/leagues/110/stats/season/27215/teams/corner_taken_team/ligue-2-teams"
        }
    },
    "Colombia": {
        "Primera A": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/clean_sheet_team/primera-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/expected_goals_team/primera-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/expected_goals_conceded_team/primera-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/touches_in_opp_box_team/primera-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/total_yel_card_team/primera-teams",
            "possession":  "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/possession_percentage_team/primera-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/fk_foul_lost_team/primera-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/ontarget_scoring_att_team/primera-teams",
            "corners":     "https://www.fotmob.com/es/leagues/274/stats/season/24856-Clausura/teams/corner_taken_team/primera-teams"
        }
    },
    "Argentina": {
        "Liga Profesional": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/clean_sheet_team/liga-profesional-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/expected_goals_team/liga-profesional-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/expected_goals_conceded_team/liga-profesional-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/touches_in_opp_box_team/liga-profesional-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/total_yel_card_team/liga-profesional-teams",
            "possession":  "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/possession_percentage_team/liga-profesional-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/fk_foul_lost_team/liga-profesional-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/ontarget_scoring_att_team/liga-profesional-teams",
            "corners":     "https://www.fotmob.com/es/leagues/112/stats/season/24590/teams/corner_taken_team/liga-profesional-teams"
        }
    },
    "Brasil": {
        "Serie A": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/clean_sheet_team/serie-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/expected_goals_team/serie-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/expected_goals_conceded_team/serie-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/touches_in_opp_box_team/serie-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/total_yel_card_team/serie-teams",
            "possession":  "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/possession_percentage_team/serie-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/fk_foul_lost_team/serie-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/ontarget_scoring_att_team/serie-teams",
            "corners":     "https://www.fotmob.com/es/leagues/268/stats/season/25077/teams/corner_taken_team/serie-teams"
        },
        "Serie B": {
            "clean_sheets": "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/clean_sheet_team/serie-b-teams",
            "xg_for":      "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/expected_goals_team/serie-b-teams",
            "xg_against":  "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/expected_goals_conceded_team/serie-b-teams",
            "touches_opp": "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/touches_in_opp_box_team/serie-b-teams",
            "yel_cards":   "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/total_yel_card_team/serie-b-teams",
            "possession":  "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/possession_percentage_team/serie-b-teams",
            "fouls_lost":  "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/fk_foul_lost_team/serie-b-teams",
            "on_target":   "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/ontarget_scoring_att_team/serie-b-teams",
            "corners":     "https://www.fotmob.com/es/leagues/8814/stats/season/25150/teams/corner_taken_team/serie-b-teams"
        }
    }
}

# ---------------------------
# Helper: crear driver
# ---------------------------
def crear_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=options)
    return driver

# ---------------------------
# Helper: calcular número de páginas (reusa tu selector)
# ---------------------------
def detectar_paginas(soup):
    total_text = soup.select_one("span.css-1hlep8c-ToFromLabel")
    if total_text:
        try:
            total = int(total_text.get_text(strip=True).split("de")[-1].strip())
            pages = (total - 1) // 10 + 1
            return pages
        except Exception:
            return 1
    return 1

# ---------------------------
# Funciones de extracción por estadística
# Cada función recibe driver + url y devuelve lista de tuplas
# ---------------------------

def extraer_general(driver, url, extra_mode="single_stat"):
    """
    Extrae filas comunes de tipo: rank, team, valor principal.
    extra_mode opcional permite devolver diferentes columnas:
      - "single_stat": (rank, name, value)
      - "index_value": (rank, name, index, value) (para on-target con índice+valor)
      - "two_values": (rank, name, val1, val2) (para goles/xG juntos)
    """
    equipos = []
    driver.get(url)
    time.sleep(PAGE_SLEEP)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    pages = detectar_paginas(soup)

    for page in range(pages):
        if page > 0:
            driver.get(f"{url}?page={page}")
            time.sleep(PAGE_SLEEP)
            soup = BeautifulSoup(driver.page_source, "html.parser")
        filas = soup.select("a[href*='/es/teams/']")
        for fila in filas:
            try:
                rank_el = fila.select_one(".css-syzgjm-Rank")
                name_el = fila.select_one(".css-cfc8cy-TeamOrPlayerName")
                if not (rank_el and name_el):
                    continue
                rank = int(rank_el.get_text(strip=True))
                name = name_el.get_text(strip=True)
                # distintos scrapers usan distintos selectores para los valores:
                sub = fila.select_one(".css-lylvvy-SubStat span")
                val = fila.select_one(".css-1mzgpr3-StatValue span")
                if extra_mode == "single_stat":
                    value = val.get_text(strip=True) if val else (sub.get_text(strip=True) if sub else "")
                    equipos.append((rank, name, value))
                elif extra_mode == "index_value":
                    index = sub.get_text(strip=True) if sub else ""
                    value = val.get_text(strip=True) if val else ""
                    equipos.append((rank, name, index, value))
                elif extra_mode == "two_values":
                    goles = sub.get_text(strip=True) if sub else ""
                    xg = val.get_text(strip=True) if val else ""
                    equipos.append((rank, name, goles, xg))
            except Exception:
                continue
    return equipos

# wrappers por estadística (para que el main sea explícito)
def scrape_clean_sheets(driver, url):
    equipos = []
    driver.get(url)
    time.sleep(PAGE_SLEEP)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    pages = detectar_paginas(soup)

    for page in range(pages):
        if page > 0:
            driver.get(f"{url}?page={page}")
            time.sleep(PAGE_SLEEP)
            soup = BeautifulSoup(driver.page_source, "html.parser")

        filas = soup.select("a[href*='/es/teams/']")
        for fila in filas:
            try:
                rank = fila.select_one(".css-syzgjm-Rank").get_text(strip=True)
                name = fila.select_one(".css-cfc8cy-TeamOrPlayerName").get_text(strip=True)

                # 👇 ESTE es el cambio correcto para Clean Sheets
                total_matches = fila.select_one(".css-lylvvy-SubStat span").get_text(strip=True)

                equipos.append((int(rank), name, total_matches))
            except Exception:
                continue

    return equipos

def scrape_xg_for(driver, url):
    return extraer_general(driver, url, "two_values")

def scrape_xg_against(driver, url):
    return extraer_general(driver, url, "two_values")

def scrape_touches_opp(driver, url):
    return extraer_general(driver, url, "single_stat")

def scrape_yel_cards(driver, url):
    return extraer_general(driver, url, "single_stat")

def scrape_possession(driver, url):
    return extraer_general(driver, url, "single_stat")

def scrape_fouls_lost(driver, url):
    return extraer_general(driver, url, "single_stat")

def scrape_on_target(driver, url):
    return extraer_general(driver, url, "index_value")

def scrape_corners(driver, url):
    return extraer_general(driver, url, "single_stat")
def guardar_en_db(pais, liga, stat_key, filas):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DB_PATH = os.path.join(BASE_DIR, "stats.db")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stats (
            pais TEXT,
            liga TEXT,
            estadistica TEXT,
            posicion INTEGER,
            equipo TEXT,
            valor1 TEXT,
            valor2 TEXT,
            fecha TEXT
        )
    ''')

    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Borrar datos viejos para mantener siempre actualizado
    cursor.execute("""
        DELETE FROM stats 
        WHERE pais=? AND liga=? AND estadistica=?
    """, (pais, liga, stat_key))

    for fila in filas:
        if len(fila) == 3:
            pos, equipo, v1 = fila
            v2 = ""
        elif len(fila) == 4:
            pos, equipo, v1, v2 = fila
        else:
            continue

        cursor.execute("""
            INSERT INTO stats 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (pais, liga, stat_key, pos, equipo, v1, v2, fecha))

    conn.commit()
    conn.close()


# mapping de nombre-estadística -> función y encabezado para mostrar
STATS_MAP = {
    "clean_sheets": ("PARTIDOS (Total de Partidos)", scrape_clean_sheets, ["Pos","Equipo","Partidos"]),
    "xg_for": ("GOLES ESPERADOS - xG (a favor)", scrape_xg_for, ["Pos","Equipo","Goles","xG"]),
    "xg_against": ("GOLES ESPERADOS - xG (concedidos)", scrape_xg_against, ["Pos","Equipo","Goles","xG"]),
    "touches_opp": ("TOQUES EN EL ÁREA RIVAL", scrape_touches_opp, ["Pos","Equipo","Toques"]),
    "yel_cards": ("TARJETAS AMARILLAS TOTALES", scrape_yel_cards, ["Pos","Equipo","Tarjetas"]),
    "possession": ("MEDIA POSESIÓN", scrape_possession, ["Pos","Equipo","Posesión"]),
    "fouls_lost": ("FALTAS POR PARTIDO", scrape_fouls_lost, ["Pos","Equipo","Faltas"]),
    "on_target": ("DISPAROS A PUERTA (ÍNDICE | Valor)", scrape_on_target, ["Pos","Equipo","Índice","Valor"]),
    "corners": ("CORNERS TOTALES", scrape_corners, ["Pos","Equipo","Corners"])
}

# ---------------------------
# Menús simples (reutilizable)
# ---------------------------
def seleccionar_opcion(lista, prompt="Selecciona una opción: "):
    for i, item in enumerate(lista, 1):
        print(f"{i}. {item}")
    while True:
        try:
            v = int(input(prompt))
            if 1 <= v <= len(lista):
                return lista[v-1]
        except Exception:
            pass
        print("Opción inválida. Intenta de nuevo.")

# ---------------------------
# MAIN: selecciona país -> liga -> extrae todas las estadísticas definidas para esa liga
# ---------------------------
import sys

def main(pais=None, liga=None):
    os.system('cls' if os.name == 'nt' else 'clear')
    print("🌐 SCRAPER MASTER - Modo Automático\n")

    # ✅ MODO AUTOMÁTICO (cuando lo llama la APP)
    if pais and liga:
        if pais not in LIGAS_UNIFICADAS:
            print(f"❌ País no válido: {pais}")
            return
        if liga not in LIGAS_UNIFICADAS[pais]:
            print(f"❌ Liga no válida: {liga}")
            return

    # ✅ MODO MANUAL (solo si lo ejecutas a mano sin parámetros)
    else:
        paises = list(LIGAS_UNIFICADAS.keys())
        pais = seleccionar_opcion(paises, "Selecciona un país: ")

        ligas = list(LIGAS_UNIFICADAS[pais].keys())
        print(f"\nLigas disponibles en {pais}:")
        liga = seleccionar_opcion(ligas, "Selecciona una liga: ")

    estat_dict = LIGAS_UNIFICADAS[pais][liga]
    print(f"\n🌐 Cargando datos de {liga} ({pais})...\n")

    driver = crear_driver()

    for stat_key, url in estat_dict.items():
        if not url or stat_key not in STATS_MAP:
            continue

        title, fn, header = STATS_MAP[stat_key]
        print(f"--> Scrapeando: {title}")

        try:
            tabla = fn(driver, url)
            tabla.sort(key=lambda x: x[0] if isinstance(x[0], int) else 9999)

            guardar_en_db(pais, liga, stat_key, tabla)

            print(f"    ✅ {len(tabla)} filas guardadas en DB.")
        except Exception as e:
            print(f"    ❌ Error en {stat_key}: {e}")

    driver.quit()
    print("✅ Scraping finalizado correctamente.")

if __name__ == "__main__":
    import sys

    if len(sys.argv) >= 3:
        pais = sys.argv[1]
        liga = sys.argv[2]
        main(pais, liga)  # ✅ modo automático
    else:
        main()  # ✅ modo manual
