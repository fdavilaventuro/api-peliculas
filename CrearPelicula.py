import boto3
import uuid
import os
import json

def log_info(data):
    print(json.dumps({
        "tipo": "INFO",
        "log_datos": data
    }))

def log_error(data):
    print(json.dumps({
        "tipo": "ERROR",
        "log_datos": data
    }))

def lambda_handler(event, context):
    try:
        # Log de entrada
        log_info({"event": event})

        # Validación básica
        if "body" not in event:
            raise ValueError("El evento no contiene 'body'")

        body = event["body"]

        if isinstance(body, str):
            body = json.loads(body)

        tenant_id = body["tenant_id"]
        pelicula_datos = body["pelicula_datos"]
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            "tenant_id": tenant_id,
            "uuid": uuidv4,
            "pelicula_datos": pelicula_datos
        }

        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Log de salida exitosa
        log_info({
            "pelicula_insertada": pelicula,
            "dynamodb_response": response
        })

        return {
            "statusCode": 200,
            "pelicula": pelicula,
            "response": response
        }

    except Exception as e:
        # Log de error
        log_error({
            "mensaje": str(e),
            "event": event
        })

        return {
            "statusCode": 500,
            "error": str(e)
        }
