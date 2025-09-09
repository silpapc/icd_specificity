"""
Module for fetching specific ICD codes from a MongoDB collection.

Provides the ICDSpecificCodes class which connects to a MongoDB instance,
normalizes ICD codes, and retrieves specific related codes based on the input.

Usage:
    Run the module as a script to interactively input an ICD code and get suggestions.
"""
import json
import re
from typing import Set, Dict, Any
from pymongo import MongoClient


class ICDSpecificCodes:
    """
    Connects to a MongoDB collection to retrieve specific ICD codes by year.

    This class normalizes ICD codes and fetches related specific codes from the database,
    supporting queries for multiple years.

    Attributes:
        collection: The MongoDB collection object.
        years: List of years to filter the ICD codes.
        code_map: Cached mapping of ICD codes to their documents.
    """
    def __init__(self, mongo_uri: str, db_name: str, collection_name: str, years=None):
        client = MongoClient(mongo_uri)
        self.collection = client[db_name][collection_name]
        if years is None:
            self.years = [2024]
        elif isinstance(years, (list, tuple, set)):
            self.years = list(years)
        else:
            self.years = [years]
        self.code_map = self._build_code_map()
    
    def _build_code_map(self) -> Dict[str, Dict[str, Any]]:
        # Find documents where year is in self.years and 'specific_codes'
            docs = list(self.collection.find({
                "specific_codes": {"$exists": True}
            }, {"_id": 1, "specific_codes": 1}))
            code_map = {}
            for doc in docs:
                _id = doc.get("_id", "")
                # Expecting _id format: 'YEAR_CODE'
                parts = _id.split("_", 1)
                if len(parts) == 2:
                    year, code = parts
                    try:
                        year_int = int(year)
                    except ValueError:
                        continue
                    if year_int in self.years:
                        # Normalize code for lookup
                        norm_code = self.normalize_code(code)
                        code_map[norm_code] = doc
            return code_map

    def normalize_code(self, code: str) -> str:
        code_clean = re.sub(r"\s+", "", code.upper())
        if '.' not in code_clean and len(code_clean) >= 4:
            # Insert a dot after the 3rd character: 'E0800' → 'E08.00'
            return f"{code_clean[:3]}.{code_clean[3:]}"
        return code_clean

    def get_specific_codes(self, raw_code: str) -> Set[str]:
        """
        Retrieve a set of specific ICD codes related to the given raw ICD code.

        Args:
            raw_code (str): The input ICD code in raw format.

        Returns:
            Set[str]: A set of specific ICD codes associated with the normalized input code.
                    Returns an empty set if the code is not found.
        """
        code = self.normalize_code(raw_code)
        doc = self.code_map.get(code)
        if not doc:
            print(
                f"[ERROR] ICD code '{raw_code}' "
                f"(normalized: '{code}') not found in the database."
            )
            return set()

        results = set()
        for sc in doc.get("specific_codes", []):
            if isinstance(sc, dict):
                results.add(sc.get("code"))
            else:
                results.add(sc)
        return results


if __name__ == "__main__":
    fetcher = ICDSpecificCodes(
        mongo_uri="mongodb://192.168.35.50:27017",
        db_name="code-ez-dev",
        collection_name="icd_codes",
        years=[2023, 2024]  # Example: search for both 2023 and 2024
    )

    raw_input_code = input("Enter ICD code: ")
    suggestions = fetcher.get_specific_codes(raw_input_code)
    print(json.dumps(list(suggestions), indent=2))
