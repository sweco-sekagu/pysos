from collections import defaultdict
from datetime import date
from enum import Enum
from typing import Literal


class Provider(Enum):
    Artportalen = 1
    ClamGateway = 2
    KUL = 3
    MVM = 4


AreaType = Literal["Province", "Municipality"]


class DateFilterType(Enum):
    OverlappingStartDateAndEndDate = "OverlappingStartDateAndEndDate"


class Query(defaultdict):
    def __init__(
        self,
        providers: list[Provider] = [Provider.Artportalen],
        provinces: list[str] = [],
        municipalities: list[str] = [],
        taxons: list[int] = [],
        start_date: date | None = None,
        end_date: date | None = None,
        geometries: list[dict] = [],
    ) -> None:
        defaultdict.__init__(self, lambda: defaultdict(list))
        self["dataProvider"]["ids"].extend([p.value for p in providers])

        for fid in provinces:
            self.add_area("Province", fid)

        for fid in municipalities:
            self.add_area("Municipality", fid)

        if len(geometries) > 0:
            self.add_geometry_filter(geometries)

        if len(taxons) > 0:
            self.add_taxons(taxons)

        if start_date is not None and end_date is not None:
            self.add_date_filter(start_date, end_date)
        else:
            raise ValueError(
                "Both start and end date must be provided for a date filter"
            )

    def add_area(self, area_type: AreaType, feature_id: str) -> None:
        self["geographics"]["areas"].append(
            {"areaType": area_type, "featureId": feature_id}
        )

    def add_geometry_filter(self, geometries: list[dict]) -> None:
        self["geographics"]["geometries"].extend(geometries)

    def add_taxons(self, taxon_ids: list[int]) -> None:
        self["taxon"]["ids"].extend(taxon_ids)

    def add_date_filter(
        self,
        start_date: date,
        end_date: date,
        filter_type: DateFilterType = DateFilterType.OverlappingStartDateAndEndDate,
    ) -> None:
        self["date"] = {
            "startDate": start_date.strftime("%Y-%m-%d"),
            "endDate": end_date.strftime("%Y-%m-%d"),
            "dateFilterType": filter_type.value,
        }
