# SPDX-FileCopyrightText: 2026 University of Strasbourg
# SPDX-FileContributor: Christophe Prud'homme
# SPDX-FileContributor: Cemosis
# SPDX-License-Identifier: Apache-2.0

"""Shared image/reference resolution for runtime and kub-img workflows."""

from __future__ import annotations

import re
from pathlib import Path

from .config import DEFAULT_DOCKER_IMAGE, KubConfig, looksLikeContainerReference
from .errors import ImageNotFoundError, KubCliError


def deriveApptainerOrasReference(dockerImageReference: str) -> str:
    """Derive Apptainer ORAS source from a Docker image reference."""

    normalized = dockerImageReference.strip()
    if not normalized:
        raise KubCliError("Docker image reference cannot be empty.")

    if "@" in normalized:
        raise KubCliError(
            "Cannot derive Apptainer ORAS reference from digest-based Docker image. "
            "Provide a tag-based Docker image reference."
        )

    if normalized.startswith("oras://"):
        return normalized

    if "://" in normalized:
        raise KubCliError(
            "Docker image reference for ORAS derivation must not include a URI scheme. "
            "Use format like ghcr.io/org/image:tag"
        )

    lastSlash = normalized.rfind("/")
    lastColon = normalized.rfind(":")

    if lastColon > lastSlash:
        repository = normalized[:lastColon]
        tag = normalized[lastColon + 1 :]
    else:
        repository = normalized
        tag = "latest"

    if not repository or not tag:
        raise KubCliError(
            "Invalid Docker image reference for ORAS derivation. "
            "Expected format like ghcr.io/org/image:tag"
        )

    return f"oras://{repository}:{tag}-sif"


def normalizeDockerImageReference(reference: str, *, sourceName: str) -> str:
    """Validate and normalize a Docker/OCI image reference."""

    normalizedReference = reference.strip()
    if not normalizedReference:
        raise ImageNotFoundError(
            f"Docker image reference from {sourceName} cannot be empty."
        )

    if normalizedReference.startswith("docker://"):
        normalizedReference = normalizedReference[len("docker://") :]

    if "://" in normalizedReference:
        raise ImageNotFoundError(
            f"Invalid Docker image reference from {sourceName}: '{reference}'. "
            "Use format like ghcr.io/org/image:tag without URI scheme."
        )

    if not looksLikeContainerReference(normalizedReference):
        raise ImageNotFoundError(
            f"Invalid Docker image reference from {sourceName}: '{reference}'. "
            "Use format like ghcr.io/org/image:tag."
        )

    return normalizedReference


def _resolveDockerReference(
    config: KubConfig,
    *,
    includeImageOverride: bool,
    strictImageOverride: bool,
    strictLegacyImage: bool,
) -> str:
    candidates: list[tuple[str, str | None, bool]] = []
    if includeImageOverride:
        candidates.append(("--image", config.imageOverride, strictImageOverride))

    candidates.extend(
        [
            ("KUB_IMAGE_DOCKER/image.docker", config.imageDocker, True),
            ("KUB_IMAGE/image", config.image, strictLegacyImage),
        ]
    )

    for sourceName, candidate, strictCandidate in candidates:
        if candidate is None:
            continue
        normalizedCandidate = candidate.strip()
        if not normalizedCandidate:
            continue

        if not strictCandidate and not (
            normalizedCandidate.startswith("docker://")
            or looksLikeContainerReference(normalizedCandidate)
        ):
            # Legacy values can point to local Apptainer files.
            # Skip non-container candidates when strict checks are disabled.
            continue

        return normalizeDockerImageReference(normalizedCandidate, sourceName=sourceName)

    return normalizeDockerImageReference(
        DEFAULT_DOCKER_IMAGE,
        sourceName="DEFAULT_DOCKER_IMAGE",
    )


def resolveDockerExecutionImage(
    config: KubConfig,
    *,
    strictImageOverride: bool = True,
    strictLegacyImage: bool = True,
) -> str:
    """Resolve Docker image reference used at runtime execution."""

    return _resolveDockerReference(
        config,
        includeImageOverride=True,
        strictImageOverride=strictImageOverride,
        strictLegacyImage=strictLegacyImage,
    )


def resolveDockerUpstreamReference(
    config: KubConfig,
    *,
    includeImageOverride: bool = True,
    strictLegacyImage: bool = True,
) -> str:
    """Resolve Docker reference for image pull/info workflows."""

    return _resolveDockerReference(
        config,
        includeImageOverride=includeImageOverride,
        strictImageOverride=True,
        strictLegacyImage=strictLegacyImage,
    )


def resolveDockerReferenceForApptainerDerivation(config: KubConfig) -> str:
    return resolveDockerUpstreamReference(
        config,
        includeImageOverride=False,
        strictLegacyImage=False,
    )


def splitImageReference(reference: str) -> tuple[str, str]:
    normalized = reference.strip()
    if not normalized:
        return "kub-image", "latest"

    withoutDigest = normalized.split("@", maxsplit=1)[0]
    lastSlash = withoutDigest.rfind("/")
    lastColon = withoutDigest.rfind(":")

    if lastColon > lastSlash:
        repository = withoutDigest[:lastColon]
        tag = withoutDigest[lastColon + 1 :]
    else:
        repository = withoutDigest
        tag = "latest"

    if not repository:
        repository = "kub-image"
    if not tag:
        tag = "latest"

    return repository, tag


def sanitizePathToken(value: str) -> str:
    sanitized = re.sub(r"[^A-Za-z0-9._-]+", "-", value).strip("-")
    return sanitized or "image"


def deriveDefaultApptainerImageFilename(config: KubConfig) -> str:
    dockerReference = resolveDockerReferenceForApptainerDerivation(config)
    _, tag = splitImageReference(dockerReference)

    normalizedTag = tag
    if normalizedTag.endswith("-sif"):
        normalizedTag = normalizedTag[: -len("-sif")]
    if normalizedTag.endswith(".sif"):
        normalizedTag = normalizedTag[: -len(".sif")]
    if not normalizedTag:
        normalizedTag = "latest"

    return f"kub-{sanitizePathToken(normalizedTag)}.sif"


def deriveLegacyDefaultApptainerImageFilename(config: KubConfig) -> str:
    dockerReference = resolveDockerReferenceForApptainerDerivation(config)
    repository, tag = splitImageReference(dockerReference)
    imageName = repository.rsplit("/", maxsplit=1)[-1] or "kub-image"

    normalizedTag = tag
    if normalizedTag.endswith("-sif"):
        normalizedTag = normalizedTag[: -len("-sif")]
    if normalizedTag.endswith(".sif"):
        normalizedTag = normalizedTag[: -len(".sif")]
    if not normalizedTag:
        normalizedTag = "latest"

    return f"{sanitizePathToken(imageName)}-{sanitizePathToken(normalizedTag)}.sif"


def normalizeApptainerImageReference(
    reference: str,
    *,
    requireExistingLocalPath: bool = True,
) -> str:
    normalizedReference = reference.strip()

    if normalizedReference.startswith("docker://"):
        raise ImageNotFoundError(
            "Apptainer image reference must use oras:// (or a local .sif path), "
            "not docker://."
        )

    if normalizedReference.startswith("oras://"):
        return normalizedReference

    if "://" in normalizedReference:
        raise ImageNotFoundError(
            "Unsupported Apptainer image URI scheme. "
            "Use oras://<registry>/<image>:<tag>-sif or a local .sif path."
        )

    if looksLikeContainerReference(normalizedReference):
        return f"oras://{normalizedReference}"

    imagePath = Path(normalizedReference).expanduser()

    if requireExistingLocalPath and not imagePath.exists():
        raise ImageNotFoundError(f"Container image not found: '{imagePath}'.")

    if imagePath.exists() and imagePath.is_dir():
        raise ImageNotFoundError(
            f"Container image must be a file, got directory: '{imagePath}'."
        )

    return str(imagePath)


def resolveExplicitApptainerImage(config: KubConfig) -> str | None:
    candidates = [config.imageOverride, config.imageApptainer, config.image]

    for candidate in candidates:
        if candidate is None:
            continue

        normalizedReference = candidate.strip()
        if not normalizedReference:
            continue

        return normalizeApptainerImageReference(normalizedReference)

    return None


def resolveLocalDefaultApptainerImage(config: KubConfig) -> str | None:
    candidateFilenames = [
        deriveDefaultApptainerImageFilename(config),
        deriveLegacyDefaultApptainerImageFilename(config),
    ]

    for filename in candidateFilenames:
        candidatePath = (Path.cwd() / filename).resolve()
        if not candidatePath.exists():
            continue

        if candidatePath.is_dir():
            raise ImageNotFoundError(
                f"Container image must be a file, got directory: '{candidatePath}'."
            )

        return str(candidatePath)

    return None


def resolveApptainerExecutionImage(config: KubConfig) -> str:
    """Resolve Apptainer execution image (local path or oras:// URI)."""

    explicitImage = resolveExplicitApptainerImage(config)
    if explicitImage is not None:
        return explicitImage

    localDefaultImage = resolveLocalDefaultApptainerImage(config)
    if localDefaultImage is not None:
        return localDefaultImage

    dockerReference = resolveDockerReferenceForApptainerDerivation(config)
    return deriveApptainerOrasReference(dockerReference)


def resolveApptainerLocalImageReference(config: KubConfig) -> str:
    """Resolve local Apptainer image destination for kub-img operations."""

    candidates = [config.imageOverride, config.imageApptainer, config.image]

    for candidate in candidates:
        if candidate is None:
            continue

        normalized = candidate.strip()
        if not normalized:
            continue

        if "://" in normalized:
            continue

        return normalized

    defaultFilename = deriveDefaultApptainerImageFilename(config)
    return str((Path.cwd() / defaultFilename).resolve())
