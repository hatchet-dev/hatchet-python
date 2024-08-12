from hatchet_sdk.v2.hatchet import ComputeConfig

# TODO 
# dockerfile/image config
# do we have hatchet defaults here?

cpu = ComputeConfig(
            cpu="2",
        )

gpu = ComputeConfig(
            gpu="h100",
        )
        