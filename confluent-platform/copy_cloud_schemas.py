#!/usr/bin/env python3
"""Copy the schemas of a Confluent Cloud Schema Registry to the Confluent Platform one.

For every subject of the Cloud registry (or only the ones selected with --subject or
--prefix) the script registers all its versions, oldest first, in the Platform
registry under the same subject name. It handles:

  * all schema types (AVRO, PROTOBUF, JSON),
  * references between schemas: a referenced subject version is copied first, and
    the reference is changed to the version number it has on Platform,
  * the per-subject compatibility level, when one is set in Cloud.

Schema IDs are NOT kept: the Platform registry assigns its own IDs. When a topic is
copied with copy_cloud_topic.py, that script maps the IDs of each message. A schema
that is already registered (same text) is not created twice, so the script can be
run again safely.

Soft-deleted subjects and versions are not copied. The global compatibility level and
the mode are not changed.

Cloud settings come from /home/aio/training/confluent-python/confluent-cloud/.env
(SCHEMA_REGISTRY_ENDPOINT, SCHEMA_REGISTRY_KEY, SCHEMA_REGISTRY_SECRET). The Platform
registry is --target-schema-registry-url (default http://sa01:8081).

Examples::

    python3 copy_cloud_schemas.py --dry-run                 # list what would be copied
    python3 copy_cloud_schemas.py                           # copy every subject
    python3 copy_cloud_schemas.py --prefix datagen-orders   # subjects starting with this
    python3 copy_cloud_schemas.py --subject orders-value --subject payments-value

Check the result::

    curl -s http://sa01:8081/subjects

Exit code 1 when a version could not be copied (for example the Platform registry
refuses it because of its compatibility level); the other subjects are still copied.
"""

from __future__ import annotations

import argparse
import os
import sys

from confluent_kafka.schema_registry import Schema, SchemaReference, SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from copy_cloud_topic import load_cloud_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy all schemas of the Cloud Schema Registry to Confluent Platform."
    )
    parser.add_argument(
        "--target-schema-registry-url",
        default=os.environ.get("SCHEMA_REGISTRY_URL", "http://sa01:8081"),
    )
    parser.add_argument("--subject", action="append", default=[],
                        help="copy only this subject (repeat for several)")
    parser.add_argument("--prefix", help="copy only subjects that start with this text")
    parser.add_argument("--dry-run", action="store_true",
                        help="only list the subjects and versions; change nothing")
    return parser.parse_args()


class SchemaCopier:
    def __init__(self, source: SchemaRegistryClient, target: SchemaRegistryClient, dry_run: bool):
        self.source = source
        self.target = target
        self.dry_run = dry_run
        self.done = {}        # (subject, Cloud version) -> Platform version
        self.in_progress = set()
        self.copied = 0
        self.failures = []

    def copy_version(self, subject: str, version: int) -> int | None:
        """Copy one subject version (after the versions it references); return its Platform version."""
        key = (subject, version)
        if key in self.done:
            return self.done[key]
        if key in self.in_progress:
            raise RuntimeError(f"circular reference at {subject} version {version}")
        self.in_progress.add(key)
        try:
            registered = self.source.get_version(subject, version)
            schema = registered.schema
            references = [
                SchemaReference(ref.name, ref.subject, self.copy_version(ref.subject, ref.version))
                for ref in schema.references or []
            ]
            copy = Schema(schema.schema_str, schema.schema_type, references)
            if self.dry_run:
                platform_version = version
            else:
                self.target.register_schema(subject, copy)
                platform_version = self.target.lookup_schema(subject, copy).version
            self.done[key] = platform_version
            self.copied += 1
            suffix = f" (references: {', '.join(r.subject for r in references)})" if references else ""
            print(f"  {subject} v{version} {schema.schema_type} -> Platform v{platform_version}{suffix}",
                  flush=True)
            return platform_version
        except (SchemaRegistryError, RuntimeError) as error:
            self.failures.append(f"{subject} v{version}: {error}")
            print(f"  {subject} v{version} FAILED: {error}", file=sys.stderr, flush=True)
            return None
        finally:
            self.in_progress.discard(key)

    def copy_subject(self, subject: str) -> None:
        print(f"{subject}", flush=True)
        for version in self.source.get_versions(subject):
            if self.copy_version(subject, version) is None:
                return  # later versions would be out of order
        try:
            level = self.source.get_compatibility(subject)
        except SchemaRegistryError:
            return  # no subject level setting in Cloud: the global one applies
        if not self.dry_run:
            self.target.set_compatibility(subject, level)
        print(f"  compatibility {level}", flush=True)


def main() -> int:
    args = parse_args()
    cloud = load_cloud_config()
    source = SchemaRegistryClient(cloud.schema_registry_config())
    target = SchemaRegistryClient({"url": args.target_schema_registry_url})

    subjects = sorted(source.get_subjects())
    if args.subject:
        missing = sorted(set(args.subject) - set(subjects))
        if missing:
            print(f"Subjects not found in Cloud: {', '.join(missing)}", file=sys.stderr)
            return 1
        subjects = sorted(args.subject)
    if args.prefix:
        subjects = [s for s in subjects if s.startswith(args.prefix)]
    if not subjects:
        print("No subjects to copy.")
        return 0

    copier = SchemaCopier(source, target, args.dry_run)
    for subject in subjects:
        copier.copy_subject(subject)

    verb = "would be copied" if args.dry_run else "copied"
    print(f"\n{copier.copied} schema versions of {len(subjects)} subjects {verb} "
          f"to {args.target_schema_registry_url}.")
    if copier.failures:
        print(f"{len(copier.failures)} failed:", file=sys.stderr)
        for failure in copier.failures:
            print(f"  {failure}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
