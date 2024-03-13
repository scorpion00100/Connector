import json
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator
from airbyte_cdk.sources.streams.http.auth import NoAuth

# Basic full refresh stream
class FactorialStream(HttpStream, ABC):
    """
    This class represents a stream output by the connector.
    """

    url_base = "https://example-api.com/v1/"
    
    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(
        self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


class Customers(FactorialStream):
    primary_key = "customer_id"

    def path(
        self, stream_state: Mapping[str, Any] = None, stream_slice: Mapping[str, Any] = None, next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "customers"


# Basic incremental stream
class IncrementalFactorialStream(FactorialStream, ABC):
    state_checkpoint_interval = None

    @property
    def cursor_field(self) -> str:
        return []

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        return {}


class Employees(IncrementalFactorialStream):
    cursor_field = "start_date"
    primary_key = "employee_id"

    def path(self, **kwargs) -> str:
        return "employees"


# Add a stream for the "fac" data
class Fac(FactorialStream):
    primary_key = None
    
    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return "fac"
    
    def request_headers(
        self, 
        **kwargs
    ) -> Mapping[str, Any]:
        # Les en-têtes de la requête doivent inclure l'apikey
        return {'Authorization': f'Bearer {self.apikey}'}
    
    def parse_response(
        self, 
        response: requests.Response, 
        **kwargs
    ) -> Iterable[Mapping[str, Any]]:
        # La réponse est un objet JSON contenant les données des employés.
        # On le transforme en dictionnaire Python et on renvoie un itérable contenant ce dictionnaire.
        yield from response.json()

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        # The API does not offer pagination,
        # so we return None to indicate there are no more pages in the response
        return None
    
    
# Source
class SourceFactorial(AbstractSource):
    def __init__(self, apikey: str):
        self.apikey = apikey
        
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        api_key = config['apikey']
        authenticator = TokenAuthenticator(token=api_key)
        return [
            Customers(authenticator=authenticator),
            Employees(authenticator=authenticator),
            Fac(authenticator=authenticator)
        ]
    def generate_catalog(self) -> Mapping[str, Any]:
        factorial_data = {
            "fac": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "first_name": {"type": "string"},
                    "last_name": {"type": "string"},
                    "full_name": {"type": "string"},
                    "email": {"type": "string", "format": "email"},
                    "birthday_on": {"type": "string", "format": "date"},
                    "terminated_on": {"type": ["null", "string"], "format": "date"},
                    "termination_reason": {"type": ["null", "string"]},
                    "termination_reason_type": {"type": ["null", "string"]},
                    "termination_observations": {"type": ["null", "string"]},
                    "termination_type_description": {"type": ["null", "string"]},
                    "identifier": {"type": "string"},
                    "identifier_type": {"type": "string"},
                    "gender": {"type": "string"},
                    "nationality": {"type": "string"},
                    "bank_number": {"type": "string"},
                    "swift_bic": {"type": "string"},
                    "bank_number_format": {"type": ["null", "string"]},
                    "country": {"type": "string"},
                    "city": {"type": "string"},
                    "state": {"type": "string"},
                    "postal_code": {"type": "string"},
                    "address_line_1": {"type": "string"},
                    "address_line_2": {"type": ["null", "string"]},
                    "company_id": {"type": "integer"},
                    "legal_entity_id": {"type": "integer"},
                    "created_at": {"type": "string", "format": "date-time"},
                    "updated_at": {"type": "string", "format": "date-time"},
                    "manager_id": {"type": "integer"},
                    "location_id": {"type": "integer"},
                    "timeoff_manager_id": {"type": ["null", "integer"]},
                    "social_security_number": {"type": ["null", "string"]},
                    "tax_id": {"type": ["null", "string"]},
                    "timeoff_policy_id": {"type": "integer"},
                    "team_ids": {"type": "array", "items": {"type": "integer"}},
                    "phone_number": {"type": "string"},
                    "company_identifier": {"type": "string"},
                    "contact_name": {"type": ["null", "string"]},
                    "contact_number": {"type": ["null", "string"]}
                },
                "required": [
                    "id", "first_name", "last_name", "full_name", "email", "birthday_on",
                    "identifier", "identifier_type", "gender", "nationality", "bank_number",
                    "swift_bic", "country", "city", "state", "postal_code", "address_line_1",
                    "company_id", "legal_entity_id", "created_at", "updated_at", "manager_id",
                    "location_id", "timeoff_policy_id", "team_ids", "phone_number", "company_identifier"
                ]
            }
        }

        streams = []
        for stream_name in factorial_data.keys():
            stream = {
                "name": stream_name,
                "json_schema": {},  # Ajoutez le schéma JSON approprié ici
                "supported_sync_modes": ["full_refresh"]
            }
            streams.append(stream)

        catalog = {
            "streams": streams
        }
        return catalog
    
    def read_catalog(self, args) -> Mapping[str, Any]:
        catalog = self.generate_catalog()
        return {"type": "CATALOG", "catalog": catalog}
    
    def generate_schema(self, data: Mapping[str, Any]) -> Mapping[str, Any]:
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "properties": {
                "id": {"type": ["null", "string"]},
                "first_name": {"type": ["null", "string"]},
                "last_name": {"type": ["null", "string"]},
                "full_name": {"type": ["null", "string"]},
                "email": {"type": ["null", "string"]},
                "birthday_on": {"type": ["null", "string"], "format": "date"},
                "terminated_on": {"type": ["null", "string"], "format": "date"},
                "termination_reason": {"type": ["null", "string"]},
                "termination_reason_type": {"type": ["null", "string"]},
                "termination_observations": {"type": ["null", "string"]},
                "termination_type_description": {"type": ["null", "string"]},
                "identifier": {"type": ["null", "string"]},
                "identifier_type": {"type": ["null", "string"]},
                "gender": {"type": ["null", "string"]},
                "nationality": {"type": ["null", "string"]},
                "bank_number": {"type": ["null", "string"]},
                "swift_bic": {"type": ["null", "string"]},
                "bank_number_format": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
                "city": {"type": ["null", "string"]},
                "state": {"type": ["null", "string"]},
                "postal_code": {"type": ["null", "string"]},
                "address_line_1": {"type": ["null", "string"]},
                "address_line_2": {"type": ["null", "string"]},
                "company_id": {"type": ["null", "integer"]},
                "legal_entity_id": {"type": ["null", "integer"]},
                "created_at": {"type": ["null", "string"], "format": "date-time"},
                "updated_at": {"type": ["null", "string"], "format": "date-time"},
                "manager_id": {"type": ["null", "integer"]},
                "location_id": {"type": ["null", "integer"]},
                "timeoff_manager_id": {"type": ["null", "integer"]},
                "social_security_number": {"type": ["null", "string"]},
                "tax_id": {"type": ["null", "string"]},
                "timeoff_policy_id": {"type": ["null", "integer"]},
                "team_ids": {"type": ["null", "array"], "items": {"type": "integer"}},
                "phone_number": {"type": ["null", "string"]},
                "company_identifier": {"type": ["null", "string"]},
                "contact_name": {"type": ["null", "string"]},
                "contact_number": {"type": ["null", "string"]}
            }
        }
        return schema
