
import os
import asyncio
import zipfile
import joblib
from sentence_transformers import SentenceTransformer
#from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
load_dotenv() 

async def load_models_bg(fastapi_app):
    """
    Run your existing init_models in the background
    using asyncio.to_thread so the event loop isn't blocked.
    """
    try:
        minilm_model, lr_classifier = await init_models()

        fastapi_app.state.minilm_model_encoder_for_clf_classifier = minilm_model
        fastapi_app.state.lr_classifier = lr_classifier

        print("Models loaded successfully!")

    except Exception as e:
        print(f"Model loading failed: {e}")

    finally:
        # Signal that models are ready
        fastapi_app.state.models_loaded_event.set()

# ---------------------- Helper for Classification model loading---


def download_model_from_blob(local_path, container_name):
    """Download all files from Azure Blob Storage container into local_path."""
    os.makedirs(local_path, exist_ok=True)

    connection_string = os.environ.get("AZURE_STORAGE_CONNECTION_STRING")
    print("conn: ", connection_string)
    print("+++++++++++++++++")
    if not connection_string:
        raise ValueError("AZURE_STORAGE_CONNECTION_STRING environment variable is not set.")

    #container_name = "multilingual-e5-small-offline"  # actual container name

    try:
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        container_client = blob_service_client.get_container_client(container_name)

        # List all blobs in the container (including subfolders)
        blobs = container_client.list_blobs()

        # If there are no blobs, warn
        if not any(True for _ in blobs):
            print(f"Warning: No blobs found in container '{container_name}'")
            return

        # Reset iterator (because we exhausted it in the check above)
        blobs = container_client.list_blobs()
        extract_flag = None
        for blob in blobs:
            # Preserve subfolder structure
            local_file = os.path.join(local_path, blob.name)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)

            print(f"Downloading {blob.name} → {local_file}")
            with open(local_file, "wb") as f:
                f.write(container_client.download_blob(blob.name).readall())
            
            if blob.name.lower().endswith(".zip"):
                extract_flag = os.path.join(local_path, ".extracted")

                # Avoid extracting every startup
                if not os.path.exists(extract_flag):
                    print(f"Extracting {local_file} ...")
                    with zipfile.ZipFile(local_file, "r") as zip_ref:
                        zip_ref.extractall(local_path)
                    open(extract_flag, "w").close()
                    print(f"Extraction complete → {local_path}")
                else:
                    print("ZIP already extracted; skipping extraction.")
            print(f"All blobs downloaded to {local_path}")

    except Exception as e:
        print(f"Error accessing container '{container_name}': {e}")



async def init_models():
    # Model paths
    minilm_path = "/tmp/minilm"                    #  Ez lesz az encoder a topicoknál
    lr_path = "/tmp/lr"                            #  Ez pedig klassifikálja a encodolt topic vektorokat

    # Download all blobs
    await asyncio.to_thread(download_model_from_blob, minilm_path, "standaloneornot-mini-ml-forencode")
    await asyncio.to_thread(download_model_from_blob, lr_path, "topicclassifiermodel")

    # --- Load the models ---
    minilm_model = await asyncio.to_thread(SentenceTransformer, minilm_path)

    classifier_file = os.path.join(lr_path, "topic_classifier.pkl")
    lr_classifier = await asyncio.to_thread(joblib.load, classifier_file)


    return minilm_model, lr_classifier


