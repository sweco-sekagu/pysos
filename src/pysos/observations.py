import json
from pathlib import Path

import requests

from pysos.querybuilder import AreaType, Query

OBSERVATION_LIMIT = 10000
OBSERVATION_TAKE = 1000
DOWNLOAD_LIMIT = 25000


class ObservationManager:
    def __init__(self, base_url: str, api_key: str) -> None:
        self.base_url = base_url
        self.session = requests.session()
        self.session.headers = {
            "X-Api-Version": "1.5",
            "Ocp-Apim-Subscription-Key": api_key,
        }

    def get_area_id(self, area_type: AreaType, area_name: str) -> str:
        params: dict[str, int | str] = {
            "areaTypes": area_type,
            "searchString": area_name,
            "skip": 0,
            "take": 1,
        }
        response = self.session.get(
            self.base_url + "/Areas",
            params=params,
        )
        response.raise_for_status()
        response_records: list[dict] = response.json()["records"]
        for area_record in response_records:
            return area_record["featureId"]
        raise RuntimeError("Area not found")

    def get_count(self, query: Query) -> int:
        response = self.session.post(
            self.base_url + "/Observations/Count",
            data=json.dumps(query),
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        return int(response.content)

    def get_observations(self, query: Query) -> list[dict]:
        count = self.get_count(query)
        if count == 0:
            raise RuntimeError("No records returned")
        elif count > OBSERVATION_LIMIT:
            raise RuntimeError("Too many records returned")
        else:
            query.update(
                {
                    "output": {
                        "fields": [
                            "datasetName",
                            "location.province",
                            "location.county",
                            "location.municipality",
                            "location.locality",
                            "location.locationId",
                            "location.coordinateUncertaintyInMeters",
                            "taxon.vernacularName",
                            "taxon.scientificName",
                            "occurrence.occurrenceId",
                            "occurrence.reportedBy",
                            "occurrence.individualCount",
                            "occurrence.sex",
                            "occurrence.lifeStage",
                            "occurrence.activity",
                            "occurrence.occurrenceRemarks",
                            "event.startDate",
                            "event.endDate",
                        ]
                    }
                }
            )

            skip = 0
            records: list[dict] = []

            while skip < count:
                response = self.session.post(
                    self.base_url + "/Observations/Search",
                    params={"skip": skip, "take": OBSERVATION_TAKE},
                    data=json.dumps(query),
                    headers={"Content-Type": "application/json"},
                )
                response.raise_for_status()
                response_record: dict = response.json()["records"]
                records.extend(response_record)
                skip += OBSERVATION_TAKE

            return records

    def download_csv(
        self,
        query: Query,
        file_path: Path | str,
        zip: bool = True,
        output_fields: list | None = None,
    ) -> None:
        count = self.get_count(query)
        if count == 0:
            raise RuntimeError("No records returned")
        elif count > DOWNLOAD_LIMIT:
            raise RuntimeError("Too many records for export")

        if output_fields:
            query.update({"output": {"fields": output_fields}})

        response = self.session.post(
            self.base_url + "/Exports/Download/Csv",
            params={"gzip": zip},
            data=json.dumps(query),
            headers={"Content-Type": "application/json"},
        )

        with open(file_path, "wb") as f:
            f.write(response.content)
