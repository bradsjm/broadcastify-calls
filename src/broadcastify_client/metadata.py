"""Helpers for decoding Broadcastify metadata payloads into typed models."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from .models import (
    AgencyDescriptor,
    CallMetadata,
    ChannelDescriptor,
    Extras,
    LocationDescriptor,
    PlaylistReference,
)


def parse_call_metadata(payload: Mapping[str, Any] | None) -> CallMetadata:
    """Convert a raw metadata mapping into a :class:`CallMetadata` instance."""
    if payload is None:
        return CallMetadata(extras=Extras())
    working: dict[str, str] = {}
    for key, value in payload.items():
        if value is None:
            continue
        working[str(key)] = str(value)

    playlist = _decode_playlist_reference(working)
    agency = _decode_agency(working)
    location = _decode_location(working)
    channel = _decode_channel(working)

    extras = Extras(MappingProxyType(dict(working)))
    return CallMetadata(
        agency=agency,
        playlist=playlist,
        location=location,
        channel=channel,
        extras=extras,
    )


def _decode_playlist_reference(values: dict[str, str]) -> PlaylistReference | None:
    identifier = _pop_first(values, "playlistUUID", "playlist_uuid")
    if identifier is None:
        return None
    name = _pop_first(values, "playlistName", "playlist_name")
    description = _pop_first(values, "playlistDescription", "playlist_description")
    return PlaylistReference(playlist_id=identifier, name=name, description=description)


def _decode_agency(values: dict[str, str]) -> AgencyDescriptor | None:
    name = _pop_first(values, "agencyName", "agency", "agency_name")
    service_description = _pop_first(values, "agencyService", "agency_service")
    call_sign = _pop_first(values, "agencyCallSign", "agency_call_sign")
    if name is None and service_description is None and call_sign is None:
        return None
    return AgencyDescriptor(name=name, service_description=service_description, call_sign=call_sign)


def _decode_location(values: dict[str, str]) -> LocationDescriptor | None:
    city = _pop_first(values, "city", "agencyCity")
    county = _pop_first(values, "county", "agencyCounty")
    state = _pop_first(values, "state", "agencyState")
    country = _pop_first(values, "country", "agencyCountry")
    latitude = _pop_float(values, "latitude", "lat")
    longitude = _pop_float(values, "longitude", "lon", "lng")
    if all(item is None for item in (city, county, state, country, latitude, longitude)):
        return None
    return LocationDescriptor(
        city=city,
        county=county,
        state=state,
        country=country,
        latitude=latitude,
        longitude=longitude,
    )


def _decode_channel(values: dict[str, str]) -> ChannelDescriptor | None:
    talkgroup_name = _pop_first(
        values,
        "talkgroupDescription",
        "talkgroup_name",
        "talkgroup",
        "alphaTag",
        "alpha_tag",
    )
    service_description = _pop_first(values, "serviceDescription", "service_description")
    service_tag = _pop_first(values, "serviceTag", "service_tag", "serviceType", "service_type")
    system_name = _pop_first(values, "systemName", "system_name")
    frequency_hz = _pop_float(values, "frequencyHz", "frequency_hz", "freq", "frequency")
    if all(
        item is None
        for item in (
            talkgroup_name,
            service_description,
            service_tag,
            system_name,
            frequency_hz,
        )
    ):
        return None
    return ChannelDescriptor(
        talkgroup_name=talkgroup_name,
        service_description=service_description,
        service_tag=service_tag,
        system_name=system_name,
        frequency_hz=frequency_hz,
    )


def _pop_first(mapping: dict[str, str], *keys: str) -> str | None:
    for key in keys:
        if key in mapping:
            return mapping.pop(key)
    return None


def _pop_float(mapping: dict[str, str], *keys: str) -> float | None:
    for key in keys:
        if key in mapping:
            raw_value = mapping.pop(key)
            try:
                return float(raw_value)
            except ValueError:
                mapping[key] = raw_value
                return None
    return None
