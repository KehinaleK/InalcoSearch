import argparse
from bs4 import BeautifulSoup
import requests
from selenium import webdriver
from selenium.webdriver.common.by import By
from requests.exceptions import RequestException
from selenium.webdriver.chrome.options import Options
import urllib.parse
from urllib.parse import  urljoin
from googleapiclient.discovery import build

"""Ce script concerne la première partie du TP numéro 3 : 
`Les sombres secrets de l'INALCO.'
Vous trouvrez un argparse en bas de ce script afin de lancer
le code et d'obtenir les outpurs pour la section désirée (1, 2 ou 3)"""

# j'ai voulu utilisé sélénium ici parce que marrant
def give_me_my_links_pls(url):
    """Cette fonction prend en entrée l'url d'une page et
    renvoie en sortie la liste des urls contenues dans cette dernière."""
    
    driver = webdriver.Chrome()
    driver.get(url)
    list_links = driver.find_elements(By.TAG_NAME, 'a')

    for link in list_links[:10]: # Jusqu'à 10 pour ne pas se faire arrêter.
        print(link.get_attribute('href'))

    driver.quit()

# on repasse à beautiful soup
def explore_links(url, depth, headers, visited=None):
    """Cette fonction prend en entrée l'url d'une page et explore
    tous les urls contenues dans celle-ci et ce de manière récursive
    pour une profondeur n donnée en argument."""

    if visited is None:
        visited = set() 

    if depth <= 0 or url in visited:
        return set()

    visited.add(url)
    # print(f"On explore l'url : {url}") 

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'lxml')
    else:
        return set()
  
    # On trouve tous les liens
    list_links = soup.find_all("a")
    list_urls = [link.get("href") for link in list_links if link.get("href")]

    # On les nettoie (on rajoute le base url s'il faut et on enlève les / en trop)
    list_urls = [url + link if link.startswith("/") else link for link in list_urls]
    list_urls = [link for link in list_urls if link.startswith("http")]

    found_urls = set(list_urls)
    
    # print(f"Les liens trouvés dans l'url {url} sont :")
    # for found_url in found_urls:
    #     print(found_url)

    to_explore = list(found_urls)  # Un copie pour pouvoir update found_urls

    for link_url in to_explore: 
        child_urls = explore_links(link_url.rstrip("/"), depth=depth-1, headers=headers, visited=visited)
        found_urls.update(child_urls)

    return found_urls

### Pour la partie 2
def query(site, extenstion):
    """Cette fonction prend un entrée une adresse de site et une extension de fichier.
    Elle permet d'obtenir la requête google permettant de renvoyer tous les fichiers
    de l'extension donnée en argument au sein du site donné en argument."""

    query_to_use = f"site:{site} filetype:{extenstion}"
    
    return query_to_use


def return_100_first_pdfs(api_key, se_id, extension):

    service = build("customsearch", "v1", developerKey=api_key)
    results = service.cse().list(q=f"filetype:{extension.strip('.')}", cx=se_id, start=1, num=10).execute()

    return results["items"]


def main():

    parser = argparse.ArgumentParser(
    description="""Ce script correspond aux questions de la partie I sur l'exploration du site de l'INALCO.
                J'ai décidé d'inclure une gestion des arguments pour que vous puissiez accéder aux outputs 
                des sections plus simplement. Bonne lecture et bonne correction !""")
    parser.add_argument("-P", "--partie", required=True, choices=["1", "2", "3"],
                        help="""Vous pouvez choisir la partie dont vous désirez l'ouput ! \n
                        Exemple d'utilisation : python3 part_1.py -P 2.""")
    
    args = parser.parse_args()

    if args.partie == "1":

        headers = {"User-Agent" : "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:132.0) Gecko/20100101 Firefox/132.0",
           "Accept" : "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Language": "en-GB,en;q=0.5",
           "Accept-Encoding" : "gzip, deflate, br, zstd",
           "Connection": "keep-alive"}

        print("RÉPONSES POUR LA PARTIE 1 :\nPetite cartographie du site de l'INALCO.\n\n")
        print("1.1 - Écrivez une fonction qui prend en entrée une url et renvoie\nen sortie la liste des urls contenues dans celle-ci :\n")
        give_me_my_links_pls("https://www.inalco.fr/")
        print("\n")
        print("1.2 - Écrivez une fonction récursive qui fait comme avant mais \nexplore en plus tous les urls du site à une\nprofondeur n donnée en argument.")
        print("Pour une profondeur de 2 (vous pouvez décommenter les prints\ndans la fonction explore_links mais l'output est assez grand !) :")
        result = explore_links("https://www.inalco.fr", 2, headers)
        print(f"{len(result)} liens ont été explorés !\n\n ")
        print("2.1 - Écrire une fonction qui renvoie pour chaque extension de fichier\nla requête Google à tapper pour trouver les fichiers avec cette extension\nsur le site de l'Inalco.")
        file_extensions = {
            ".pdf": "Portable Document Format",
            ".docx": "Microsoft Word document (newer format)",
            ".doc": "Microsoft Word document (older format)",
            ".csv": "Comma-separated values file for tabular data",
            ".tsv": "Tab-separated values file for tabular data",
            ".jpg": "JPEG image file",
            ".png": "Portable Network Graphics image file",
            ".tiff": "Tagged Image File Format image",
            ".bmp": "Bitmap image file",
            ".ppt": "Microsoft PowerPoint presentation (older format)",
            ".pptx": "Microsoft PowerPoint presentation (newer format)",
            ".pptm": "Microsoft PowerPoint macro-enabled presentation",
            ".odt": "OpenDocument Text document",
        }
        for key in file_extensions.keys():
            query_to_use = query("inalco.fr", key.strip("."))
            print(f"Query pour l'extension '{key}' :")
            print("\t", query_to_use)
        print("\n")
        print("2.2 - Écrivez une fonction qui retourne l'url des 100 premiers fichiers\npdfs du site de l'INALCO.")
        results = return_100_first_pdfs("AIzaSyBWwSI_KPQRkKUyigLYYFWY9RYoLROZDMM", "60ecda80a98954bc2", "pdf")
        for result in results[:20]: # parce qu'un peu long 
            print(result["title"], result["link"])


if __name__ == "__main__":  
    main()