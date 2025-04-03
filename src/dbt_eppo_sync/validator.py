# dbt_eppo_sync/validator.py

import jsonschema
from jsonschema.exceptions import ValidationError
from typing import List, Dict, Any

def _format_validation_error(error: ValidationError) -> str:
    """Formats a jsonschema ValidationError into a user-friendly string."""
    # error.path provides the location of the error in the data
    path_str = "$." + ".".join(map(str, error.path))
    # error.message describes the validation failure
    # error.schema_path shows the path in the schema that failed
    # error.context can provide more details for complex errors
    return f"Validation Error at '{path_str}': {error.message}"

def validate_eppo_payload(payload: Dict[str, Any], schema: Dict[str, Any]) -> List[str]:
    """
    Validates the generated Eppo payload against the provided JSON schema.

    Args:
        payload: The generated payload dictionary to validate.
        schema: The Eppo JSON schema dictionary.

    Returns:
        A list of strings describing validation errors. Returns an empty list
        if the payload is valid according to the schema.
    """
    errors = []
    try:
        # Create a validator instance (optional, but allows customization)
        # Using Draft7Validator as specified in the schema's $schema keyword
        validator = jsonschema.Draft7Validator(schema)

        # Iterate through errors instead of stopping at the first one
        validation_errors = sorted(validator.iter_errors(payload), key=str)

        for error in validation_errors:
            errors.append(_format_validation_error(error))

    except jsonschema.exceptions.SchemaError as e:
        # This indicates the schema itself is invalid
        errors.append(f"Schema Error: The provided Eppo schema is invalid. {e}")
    except Exception as e:
        # Catch other potential errors during validation
        errors.append(f"An unexpected error occurred during validation: {e}")

    return errors

