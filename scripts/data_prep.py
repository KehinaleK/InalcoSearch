import argparse
import glob
import os
from datetime import datetime
from unstructured.partition.auto import partition
import chromadb
from chromadb.config import Settings
import time
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio
import asyncio
import json
import logging
from tqdm import tqdm

"""Ce script concerne la deuxième partie (La Data Prep) du TP numéro 3 : 
`Les sombres secrets de l'INALCO.'
Vous trouvrez un argparse en bas de ce script afin de lancer
le code et d'obtenir les outpurs pour la section désirée (4, 5, 6, 7 ou 8)"""

#======================================================#
#======================================================#
#========              CLASSES                 ========#
#======================================================#
#======================================================#

class Document:

    """Cette classe permet de stocker les informations relatives à un document."""

    num_file = 0

    def __init__(self, file_id = "", collect_date = "", modification_date = "", size = "", text = "", num_file = num_file, file_name = ""):
        self.file_id = file_id
        self.collect_date = collect_date
        self.modification_date = modification_date
        self.size = size
        self.text = text
        self.num_file = num_file
        self.file_name = file_name

    def _get_id(self, num_file, file_name, c_time):
        clean_title = file_name.split("/")[-1].split(".")[0]
        clean_title = clean_title.replace("_", "").replace("_", "")
        if len(clean_title) >= 10:
            clean_title = clean_title[:10]
        return f"Inalco_{num_file}_{c_time}_{clean_title}"
        
    def _add(self, file_name, file_content):
        stats = os.stat(file_name)
        self.file_name = file_name
        c_time = stats.st_mtime
        m_time = stats.st_ctime
        c_time = (datetime.fromtimestamp(c_time)).date()
        m_time = (datetime.fromtimestamp(m_time)).date()
        self.collect_date = c_time.strftime('%m/%d/%Y')
        self.modification_date = m_time.strftime('%m/%d/%Y')
        self.size = stats.st_size / 1024
        self.text = file_content
        self.file_id = self._get_id(self.num_file, file_name, c_time)
        Document.num_file += 1

    def __str__(self):
        return f"File ID : {self.file_id}\nCollection Date : {self.collect_date}\nModification Date : {self.modification_date}\nSize : {self.size} kb\nText : {self.text[:100]}"
        
class Chunk:

    """Cette classe permet de stocker les informations relatives à un chunk d'un document"""

    def __init__(self, num_chunk, chunk_file, chunk_id = "", text = "", length = 0):
        self.num_chunk = num_chunk
        self.chunk_file = chunk_file
        self.chunk_id = chunk_id
        self.text = text
        self.length = length
      
    def _get_id(self, file_id, chunk_num):
        return file_id + "_" + str(chunk_num)
        
    def _add(self, file_id, chunk_content):
        self.text = chunk_content
        self.length = len(chunk_content)
        self.chunk_id = self._get_id(file_id, self.num_chunk)
    
    def __str__(self):
        return f"Chunk ID : {self.chunk_id}\nLength : {self.length}\nText : {self.text}"

#======================================================#
#======================================================#
#========             DÉCORATEURS              ========#
#======================================================#
#======================================================#

def get_report(func):
    """Ce décorateur permet de monitorer le temps d'exécution d'une fonction"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        return result, execution_time
    return wrapper

def async_get_report(func):
    """Ce décorateur permet de monitorer le temps d'exécution d'une fonction asynchrone"""
    async def wrapper(*args, **kwargs):
        start_time = time.time()  
        result = await func(*args, **kwargs)  
        end_time = time.time() 
        execution_time = end_time - start_time
        return result, execution_time  
    return wrapper

#======================================================#
#======================================================#
#========             FONCTIONS                ========#
#======================================================#
#======================================================#

def extract_text(file_name):
    """Cette fonction permet d'extraire le texte d'un fichier pdf ou html"""
    logging.info(f"Extraction du texte du fichier {file_name} !")
    file_content = partition(file_name)
    text = "[SEP]".join([str(el) for el in file_content])

    return text

def chunking_un_max(text, size):
    """Cette fonction permet d'adapter le chunking en fonction de la taille du texte"""

    logging.info("Chunking du texte extrait précédemment !")
    list_chunks = []
    if "[SEP]" in text:
        segments = text.split("[SEP]")
    else:
        if "." in text:
            segments = text.split(".")
        else:
            segments = text.split()

    jumps = 0
    for index, segment in enumerate(segments):
        if jumps > 0:
            jumps -= 1
            continue
        if len(segment) < 500:
            new_segment = segment
            i = 1
            while index + i < len(segments):
                if len(new_segment + segments[index + i]) < 500:
                    new_segment += " " + segments[index + i]
                    i += 1
                    jumps += 1
                else:
                    break
                
            list_chunks.append(new_segment)

        else:
            chunks = chunking_un_max(segment, size)
            for chunk in chunks:
                list_chunks.append(chunk)

    return list_chunks

def get_document_chunks(Document):
    """Cette fonction permet d'obtenir les chunks d'un document"""

    text = Document.text
    chunk_file = Document.file_name
    chunks = chunking_un_max(text, 500)

    Document_chunks = []

    logging.info("Création des chunks !")
    for num_chunk, chunk in enumerate(chunks):
        chunk_object = Chunk(num_chunk, chunk_file)
        chunk_object._add(Document.file_id, chunk)
        Document_chunks.append(chunk_object)
        
    return Document_chunks

def create_database(name, folder_path):
    """Cette fonction permet de créer une base de données chromadb"""

    chroma_client = chromadb.Client(Settings(persist_directory=folder_path))
    collection = chroma_client.get_or_create_collection(name=name)

    return collection #retourne l'objet collection dans lequel on store nos données ! 


def stock_chunk(chunks, collection):
    """Cette fonction permet de stocker les chunks d'un document dans la base de données"""

    ids = []
    documents = []
    file_names = []

    logging.info("Stockage des chunks dans la base de données !")
    for chunk in chunks:
        ids.append(chunk.chunk_id)
        documents.append(chunk.text)
        file_names.append(chunk.chunk_file)
        
    
    metadatas = [{"file_name" : file_name} for file_name in file_names]

    collection.add(
            ids = ids,
            documents=documents,
            metadatas=metadatas,
    )

#======================================================#
#======================================================#
#========    SCRIPT POUR INDEXER UN FICHIER    ========#
#======================================================#
#======================================================#

@get_report
def index_file(file_name, collection):
    """Cette fonction permet d'obtenir les chunks d'un document et de les stocker dans la base de données"""

    num_empty_chunks = 0
    corrupted_file = False

    document = Document(file_name)
    document._add(file_name, extract_text(file_name))
    logging.info(f"Document : {document.file_name}")
    if document.text is None:
        corrupted_file = True #si extraction pas bien passée
        print("Le fichier est corrompu !")
        print(document.text)
        
    chunks = get_document_chunks(document)

    for chunk in chunks:
        if len(chunk.text) == 0:
            num_empty_chunks += 1 #si un chunk vide !
    
    stock_chunk(chunks, collection)

    return num_empty_chunks, corrupted_file

#======================================================#
#======================================================#
#========    SCRIPT POUR INDEXER UN DOSSIER    ========#
#======================================================#
#======================================================#

@get_report
def index_all_folder(folder_name, collection):
    """Cette fonction permet d'indexer tous les fichiers d'un dossier"""

    monitoring_report = defaultdict(dict)
    all_files = glob.glob(folder_name)

    for file in tqdm(all_files):
        result, execution_time = index_file(file, collection)
        num_empty_chunks, corrupted_file = result
        monitoring_report[file]["Execution Time"] = f"{execution_time:.2f}"
        monitoring_report[file]["Corrupted File"] = corrupted_file
        monitoring_report[file]["Empty Chunks"] = num_empty_chunks
    
    total_corrupted_files = len(list(filter(lambda x: x["Corrupted File"] == True, monitoring_report.values())))
    total_empty_chunks = sum([x["Empty Chunks"] for x in monitoring_report.values()])

    return total_corrupted_files, total_empty_chunks, monitoring_report

### VERSIONS ASYNCHRONES ###
### JUSTE POUR ILLUSTRER À QUOI RESSEMBLERAIT LE CODE SI ON VOULAIT L'UTILISER AVEC ET SANS DE L'ASYNC ###

@async_get_report
async def index_file(file_name, collection):
    """Cette fonction permet d'obtenir les chunks d'un document et de les stocker dans la base de données de manière asynchrone"""

    num_empty_chunks = 0
    corrupted_file = False

    document = Document(file_name)
    document._add(file_name, extract_text(file_name))
    print(f"Document : {document.text[:100]}")
    if document.text is None:
        corrupted_file = True #si extraction pas bien passée
        print("Le fichier est corrompu !")
        print(document.text)
        
    chunks = get_document_chunks(document)

    for chunk in chunks:
        if len(chunk.text) == 0:
            num_empty_chunks += 1 #si un chunk vide !
    stock_chunk(chunks, collection)

    return num_empty_chunks, corrupted_file


@async_get_report
async def index_all_folder(folder_name, collection):
    """Cette fonction permet d'indexer tous les fichiers d'un dossier de manière asynchrone"""

    monitoring_report = defaultdict(dict)

    all_files = glob.glob(folder_name)

    tasks = [index_file(file, collection) for file in all_files]

    results = await asyncio.gather(*tasks, return_exceptions=True)
 
    for file, result in zip(all_files, results):
        return_values, execution_time = result
        num_empty_chunks, corrupted_file = return_values
        monitoring_report[file]["Execution Time"] = f"{execution_time:.2f}"
        monitoring_report[file]["Corrupted File"] = corrupted_file
        monitoring_report[file]["Empty Chunks"] = num_empty_chunks
    
    total_corrupted_files = len(list(filter(lambda x: x["Corrupted File"] == True, monitoring_report.values())))
    total_empty_chunks = sum([x["Empty Chunks"] for x in monitoring_report.values()])

    return total_corrupted_files, total_empty_chunks, monitoring_report

def save_collection(collection):
    """Cette fonction permet de sauvegarder la base de données dans un fichier json"""

    data = collection.get(include=["documents", "metadatas"])

    with open("../database_.json", "w") as f:
        json.dump(data, f)

def main():

    parser = argparse.ArgumentParser(
    description="""Ce script correspond aux questions de la partie I sur l'exploration du site de l'INALCO.
                J'ai décidé d'inclure une gestion des arguments pour que vous puissiez accéder aux outputs 
                des sections plus simplement. Bonne lecture et bonne correction !""")
    parser.add_argument("-P", "--partie", required=True, choices=["4", "5", "6", "7", "8"],
                        help="""Vous pouvez choisir la partie dont vous désirez l'ouput ! \n
                        Exemple d'utilisation : python3 part_2.py -P 5.""")
    
    args = parser.parse_args()

    logging.basicConfig(filename="my_app.log", level=logging.INFO)
    logging.info("Début du script !")
    
    list_pdf = glob.glob("../data/corpus/pdf_files/*.pdf") # pour obtenir tous les fichiers pdf

    if args.partie == "4":
        logging.info("Début de la partie 4 !")
        print("Réponses pour la partie 4 :")
        print("EXTRAIRE L'INFORMATION PERTINENTE DES FICHIERS")
        print("Le nombre de fichiers PDF est de : ", len(list_pdf))
        print("\n\n")
        print("4.a.1 À votre avis, quels sont les champs dont il faut doter l'objet Document ?")
        print("\tLes chamops que j'ai décidé d'inclure dans l'objet Document sont :")
        print("\t- Le titre du document")
        print("\t- La date de collecte")
        print("\t- La date de modification")
        print("\t- La taille du document (en kb)")
        print("\t- Le texte contenu dans le document")
        print("\n")
        print("4.a.2 Créez une classe Document, ainsi que les fonctions init et str et add qui ajoute un paramètre à l'objet.")
        print("\t cf. classe Document en haut du script.")
        print("\n")
        print("4.a.3 Implémentez une méthode qui clacule l'ID d'un document.")
        print("\t cf. méthode _get_id() dans la classe Document.")
        print("\n")
        print("4.b.1 Écrivez une fonction qui prend en entrée un nom de fichier\nqu'il soit html ou pdf et renvoie le texte.")
        text = extract_text(list_pdf[0])
        print("Le contenu du fichier est : ")
        print(f"\t{text}")
        print("\n")
        print("4.2.b Écrivez une fonction qui prend en entrée un chemin de fichier\n et renvoie un objet Document contenant également son texte.")
        doc = Document(list_pdf[0])
        doc._add(list_pdf[0], text)
        print("Voici le document obtenu (on output seulement le début du texte pour la clarté): ")
        print(doc)
        logging.info("Fin de la partie 4 !")

    if args.partie == "5":
        logging.info("Début de la partie 5 !")
        print("Réponses pour la partie 5 :")
        print("CHUNKING DES DOCUMENTS")
        print("\n\n")
        print("5.1 Renseignez vous sur les différentes méthodes de chunking existantes.\nLaquelle vous semble la plus pertinente ?")
        print("\tIl existe plusieurs méthodes de chunking !")
        print("\tDans le cadre d'un stage effectué cette année, j'ai été amené à utiliser l'outil SEM\nafin de faire du chunking sur des textes en français.")
        print("\tLe principe était d'essayer d'obtenir des entitées syntaxiques non récursives.")
        print("\tEn d'autres termes, d'obtenir les plus petites unités porteuses de sens pour une étude qui\nincluait ici la syntaxe et la socio-linguistique.")
        print("\tJe pense que cette méthode n'est pas la plus pertinente ici. Afin d'optimiser un moteur de recherche,\nil semblerait que diviser les chunks en fonction de leur taille soit plus pertinent.")
        print("\tMicrosoft (https://learn.microsoft.com/en-us/azure/search/vector-search-how-to-chunk-documents), semble\npar exemple conseiller de garder des paragraphes afin de ne pas séparer\ndes unités sémantiques.")
        print("\tIl semble en effet logique d'imaginer que les paragraphes sont construits de manière à\norganiser et structurer des idées et donc des unités sémantiques.")
        print("\tCela semble pertinent pour un moteur de recherche et PARTITION divise déjà les documents en fonction\tdes sections de mise en forme des fichiers pdf et html.")
        print("\tNous pouvons imaginer que ces sections reflètent des unités sémantiques (les paragraphes, les titres, etc sont par exemple séparés).")
        print("\tGarder les séparateurs [SEP] de PARTITION me parait donc ici plus que pertinent !")
        print("\n")
        print("5.2 Implémentez une prend en entrée un texte et renvoie la liste de chunks.")
        print("Tant que le chunk ne dépasse par n caractères (initialisé à 500), on continue de le remplir.")
        print("\t cf. fonction chunking_un_max().")
        print("\n")
        print("5.3 Créez une classe Chunk avec les champs qui vous semblent pertinents et les méthodes init et str.")
        print("\tUne objet chunk est défini par :")
        print("\t- Un ID.")
        print("\t- Un texte.")
        print("\t- Sa longueur.")
        print("Cf. classe Chunk plus haut dans le script.")
        print("\n")
        print("5.4 Écrivez une fonction qui prend en entrée un Document\net renvoie la liste des objets Chunks correspondant à ce document.")
        print("\tVoici un exemple de chunking pour un document (Cf. get_document_chunks()) :")
        doc = Document(list_pdf[0])
        doc._add(list_pdf[0], extract_text(list_pdf[0]))
        chunks = get_document_chunks(doc)
        for chunk in chunks[:2]:
            print(f"Chunk ID : {chunk.chunk_id}")
            print(f"Longueur du chunk : {chunk.length}")
        
        logging.info("Fin de la partie 5 !")
    
    logging.info("La database est créée !")
    collection = create_database("DatabaseInalco", "chromadb_data")

    if args.partie == "6":
        logging.info("Début de la partie 6 !")
        print("Réponses pour la partie 6 :")
        print("INDEXATION DE LA BASE DE DONNÉES")
        print("\n\n")
        print("6.a.1 Écrivez une fonction pour créer la base de données et éventuellement la purger si nécessaire.")
        print("\t cf. create_database() ci-dessus.")
        print("\n")
        print("6.a.2 Écrivez une fonction qui permet de stocker un chunk.")
        print("\t cf. stock_chunk() ci-dessus.")
        print("\n")
        print("6.b.1 Écrivez une fonction qui prend en entrée un nom de fichier\net la collection, crée le document et indexe\nles chunks de la base.")
        index_file(list_pdf[0], collection)
        print("\tExemple avec un fichier pdf :")
        data = collection.get(include=["documents"])
        for key, value in data.items():
            print(key)
            if value:
                for elem in value:
                    print(elem[:30])
        print("\n")
        print("\tOn a pour l'instant stocké les id des chunks (qui sont uniques) ains que les textes et les embeddings de ces derniers !")
        print("\n")
        print("6.b.2 Écrivez un script global qui prend en entrée un nom de DOSSIER\net traite les documents du dossier de façon\nséquentielle à l'aide de la fonction précédente.")
        print("\tcf. index_all_folder() ci-dessus.")
        logging.info("Fin de la partie 6 !")

    if args.partie == "7":
        logging.info("Début de la partie 7 !")
        print("Réponses pour la partie 7 :")
        print("MONITORER SON PROGRAMME")
        print("\n\n")

        ### À DÉCOMMENTER SI VOUS VOULEZ VÉRIFIER QUE TOUT MARCHE ! ###
        # result, execution_time = index_all_folder("../inalco_files/pdf_files/*.pdf", collection)
        # total_corrupted_files, total_empty_chunks, monitoring_report = result
        # monitoring_report["General"]["Execution Time"] = f"{execution_time:.2f}"
        # monitoring_report["General"]["Total Corrupted Files"] = total_corrupted_files
        # monitoring_report["General"]["Total Empty Chunks"] = total_empty_chunks
        
        # print("7.a Écrivez un décorateur qui permet de monitorer le temps d'exécution d'une fonction\net les chunks vides ainsi que les fichiers avec une erreur.")
        # print("Faites en sorte que ce décorateur créer un rapport de monitoring pour chaque fichier traité\nainsi qu'un rapport général.")
        # print("\t cf. get_report() ci-dessus.")
        # print("\t Voici le rapport de monitoring obtenu :")
        # print("\t Le traitement global a duré : ", monitoring_report["General"]["Execution Time"], " secondes.")
        # print("\t Le nombre de fichiers corrompus est de : ", monitoring_report["General"]["Total Corrupted Files"])
        # print("\t Le nombre de chunks vides est de : ", monitoring_report["General"]["Total Empty Chunks"])
        # print("\t Le temps moyen de traitement par fichier est de : ", float(monitoring_report["General"]["Execution Time"])/len(monitoring_report), " secondes.")
        # print("\t Le temps médian de traitement par fichier est de : ", sorted([float(x["Execution Time"]) for x in monitoring_report.values()])[len(monitoring_report)//2], " secondes.")
        # save_collection(collection)

        ### Sinon, les résultats sont les suivants : ###

        print("7.a Écrivez un décorateur qui permet de monitorer le temps d'exécution d'une fonction\net les chunks vides ainsi que les fichiers avec une erreur.")
        print("Faites en sorte que ce décorateur créer un rapport de monitoring pour chaque fichier traité\nainsi qu'un rapport général.")
        print("\t cf. get_report() ci-dessus.")
        print("\t Voici le rapport de monitoring obtenu :")
        print("\t Le traitement global a duré : 3144.65 secondes (52 minutes).")
        print("\t Le nombre de fichiers corrompus est de : 0")
        print("\t Le nombre de chunks vides est de : 210")
        print("\t Le temps moyen de traitement par fichier est de : 5.08 secondes.")
        print("\t Le temps médian de traitement par fichier est de : 2.3 secondes.")

    if args.partie == "8":
        print("Réponses pour la partie 8 :")
        print("MULTITHREADING")
        print("\n\n")

        ### À DÉCOMMENTER SI VOUS VOUS VOULEZ VÉRIFIER QUE TOUT MARCHE
        # print("8.1 Renseignez vous sur les différentes méthodes de multithreading en Python.")
        # result, execution_time = asyncio.run(index_all_folder("../data/corpus/pdf_files/*.pdf", collection))
        # total_corrupted_files, total_empty_chunks, monitoring_report = result
        # print("Le temps de traitement global a été de : ", execution_time, " secondes.")
        # print("Le temps moyen de traitement par fichier est de : ", execution_time/len(monitoring_report), " secondes.")
        # print("Le temps médian de traitement par fichier est de : ", sorted([float(x["Execution Time"]) for x in monitoring_report.values()])[len(monitoring_report)//2], " secondes.")
        # print("Le nombre de fichiers corrompus est de : ", total_corrupted_files)
        # print("Le nombre de chunks vides est de : ", total_empty_chunks)
        
        ### Sinon, les résultats sont les suivants : ###
        print("\t Voici le rapport de monitoring obtenu :")
        print("\t Le traitement global a duré : 2758.5235 secondes (52 minutes).")
        print("\t Le nombre de fichiers corrompus est de : 0")
        print("\t Le nombre de chunks vides est de : 210")
        print("\t Le temps moyen de traitement par fichier est de : 4.46 secondes.")
        print("\t Le temps médian de traitement par fichier est de : 1.95 secondes.")

if __name__ == "__main__":  
    main()