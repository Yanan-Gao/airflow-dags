from dataclasses import dataclass

from kubernetes.client import V1EphemeralVolumeSource, V1Volume, V1PersistentVolumeClaimTemplate, \
    V1PersistentVolumeClaimSpec, V1ResourceRequirements, V1VolumeMount


@dataclass
class EphemeralVolumeConfig:

    name: str
    storage_request: str
    storage_class_name: str
    mount_path: str

    def create_volume(self) -> V1Volume:
        return V1Volume(
            name=self.name,
            ephemeral=V1EphemeralVolumeSource(
                volume_claim_template=V1PersistentVolumeClaimTemplate(
                    spec=V1PersistentVolumeClaimSpec(
                        access_modes=["ReadWriteOnce"],
                        resources=V1ResourceRequirements(requests={"storage": self.storage_request}),
                        storage_class_name=self.storage_class_name
                    )
                )
            )
        )

    def create_volume_mount(self) -> V1VolumeMount:
        return V1VolumeMount(
            mount_path=self.mount_path,
            name=self.name,
        )
