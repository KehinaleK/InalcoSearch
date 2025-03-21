import chromadb
from collections import defaultdict
import json
import streamlit as st
from annotated_text import annotated_text

### Ce code contient tout le code nécessaire pour obtenir nos similarités et lancer l'application streamlit.
### Ce n'est pas très beau, mais j'ai stocké la database chromadb dans un fichier Json (car je n'arrivais pas à le faire dans une persisting directory...)
### et j'utilise le contenu de ce fichier json pour recréer notre base de données et ainsi éviter de devoir lancer le script très long pour pouvoir l'utiliser. 
### Veuillez lancer ce script avant la commande : streamlit run search_engine.py

def choose_mode() -> str:
    """Cette fonction gère la sélection du mode choisi par l'utilisateur."""
    
    st.markdown("""
    <style>
    [data-testid="stSidebarContent"] {
        color: black;
        background-color: #457b9d;
    }
    </style>
    """, unsafe_allow_html=True)
    mode = st.sidebar.selectbox("", options=["Accueil", "Recherche","À propos"], format_func=lambda x: "Accueil" if x == "Accueil" else x)
    return mode


def display_home_page(mode:str):

    if mode == "Accueil":
        st.markdown("""
            <h1 style='text-align: center; color: white;'>🕵️‍♀️ Sombres Secrets de l'INALCO 🕵️‍♂️</h1>
            <h3 style='text-align: center; color: white; font-style: italic;'>Bienvenu sur votre brand-new moteur de recherche du site internet 
                de l'Institut National des Langues et Civilisations Orientales.</h3>

        <div style='margin-top: 20px; padding: 16px;'>
            <p style='color: white; text-align: justify; font-size: 16px; font-'>
                Cette application vous permet d'explorer le site internet de l'<a href='https://www.inalco.fr/'>INALCO</a> afin de retrouver des documents pouvant répondre à vos besoins.
            </p>
        </div>
        """, unsafe_allow_html=True)

        st.markdown(
            """
            <style>
                [data-testid=stImage]{
                    text-align: center;
                    display: block;
                    margin-left: auto;
                    margin-right: auto;
                    width: 100%;
                }
            </style>
            """, unsafe_allow_html=True
            )
        
        st.image("../data/img/sad_inalco.png")
    
def display_search_page():

    st.markdown("""
        <h1 style='text-align: center; color: white;'>🔍 Recherche 🔍</h1>
    """, unsafe_allow_html=True)

    query = st.text_input("Votre recherche")
    nb_results = st.slider("Nombre de résultats maximum", min_value=1, max_value=20)

    return query, nb_results

def display_about_page():

    st.markdown("""
        <h1 style='text-align: center; color: white;'>📚 À propos 📚</h1>
    """, unsafe_allow_html=True)

    st.markdown("""
        <div style='margin-top: 20px; padding: 16px;'>
            <p style='color: white; text-align: justify; font-size: 16px;'>
                Cette application a été développé dans le cadre du TP de l'UE "Langages de script" du Master Plurital. 
                Elle a pour but de permettre aux utilisateurs de retrouver des documents sur le site de l'INALCO à partir d'un moteur de recherche. 
                Ce dernier a été crée à l'aide d'une base de données ChromaDB permettant ainsi d'utiliser les similarités cosinus pour retrouver les documents les plus pertinents.
            </p>
        </div>
                """, unsafe_allow_html=True)

@st.cache_resource
def load_data(file_path):
    """Cette fonction permet de charger les données du fichier JSON"""
    with open(file_path, "r") as file:
        data = json.load(file)
    return data

@st.cache_resource
def re_create_database(data, name):
    """Cette fonction permet de recréer la base de données"""
    chroma_client = chromadb.Client()
    collection = chroma_client.get_or_create_collection(name=name)

    ids = data["ids"]
    documents = data["documents"]
    metadatas = data["metadatas"]
    
    collection.add(ids=ids[:500], documents=documents[:500], metadatas=metadatas[:500])

    return collection   


def get_results(question, collection, nb_results):
    """Cette fonction renvoie les documents les plus similaires à la query."""
    
    results = collection.query(
        query_texts=question,
        n_results=nb_results)

    return results

def get_entire_document(file_name_target, data):
    """Cette fonction renvoie le document en entier pour pouvoir l'afficher sur le site."""

    document = []

    metadatas = data["metadatas"]
    file_names = [dico["file_name"] for dico in metadatas]
    texts = data["documents"]

    for text, file_name in zip(texts, file_names):
        if file_name == file_name_target:
            document.append(text)

    return document

def display_results(display_dict):
    """Cette fonction permet d'afficher les résultats de la recherche."""

    num_result = 1


    for file_name, file_info in display_dict.items():
        st.markdown(f"### Résultat {num_result}")
        document = file_info["document"]
        chunks = file_info["chunks"]
        distances = file_info["distances"]

        text_with_annotations = []

        for doc in document:
            for chunk, distance in zip(chunks, distances):
                if doc == chunk:
                    text_with_annotations.append((doc, str(distance)))
                else:
                    text_with_annotations.append(doc)
            
            if "p." in doc:
                text_with_annotations.append("\n\n\n")
            else:
                text_with_annotations.append(" ")

        expanded_adress = st.expander("Voir l'adresse du document")
        with expanded_adress:
            st.write(file_name)
        expanded_text = st.expander("Voir le document")
        with expanded_text:
            annotated_text(*text_with_annotations)
        
        num_result += 1
        

def main():

    data = load_data("../data/database/database.json")
    collection = re_create_database(data, "DatabaseInalco")
    data = collection.get(include=["documents", "metadatas"])
    
    mode = choose_mode()
    print(mode)
    display_home_page(mode)

    if mode == "Recherche":

        query, nb_results = display_search_page()
        print(f"###{query}###")

        if query:
            
            query = [query]
            display_dict = defaultdict(lambda: {"chunks": [], "distances": []}) # Pour organiser ce qui va être affiché !!! 

            for key_word in query:

                results = get_results(key_word, collection, nb_results)
                chunks = results["documents"][0]
                metadatas = results["metadatas"][0]
                files = [dico["file_name"] for dico in metadatas]
                distances = results["distances"][0]
                
                for chunk, file, distance in zip(chunks, files, distances):
                    display_dict[file]["chunks"].append(chunk)
                    display_dict[file]["distances"].append(distance)

                
                resulting_files = display_dict.keys()

                for resulting_file in resulting_files:
                    document = get_entire_document(resulting_file, data)
                    display_dict[resulting_file]["document"] = document

                display_results(display_dict)

    elif mode == "À propos":
        display_about_page()

if __name__ == "__main__":
    main()