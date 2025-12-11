import shutil
import time

import nexusformat.nexus as nx
import numpy as np

from pathlib import Path
from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from tiled.client import from_profile
from tiled.utils import path_from_uri


BEAMLINE_OR_ENDSTATION = "arpes"

@task(retries=2, retry_delay_seconds=10)
def export_metadata_task(uid, beamline_acronym=BEAMLINE_OR_ENDSTATION):
    logger = get_run_logger()

    api_key = Secret.load(f"tiled-{beamline_acronym}-api-key", _sync=True).get()
    tiled_client = from_profile("nsls2", api_key=api_key)
    run_client = tiled_client[beamline_acronym]["migration"][uid]
    logger.info(f"Obtained a Tiled client for Bluesky Run with uid {uid}. Exporting metadata...")

    start_time = time.monotonic()

    # Find the data file that needs to be augmented with metadata
    fpath_orig = path_from_uri(run_client["primary/mbs_image"].data_sources()[0].assets[0].data_uri)
    if fpath_orig.is_file():
        logger.info(f"Found data file at {fpath_orig}, proceeding with metadata export.")
    else:
        logger.error(f"Data file not found at {fpath_orig}, aborting metadata export.")
        return
    
    # Copy the file to the destination directory and update the fpath variable
    # For example:
    # /nsls2/data3/esm/proposals/commissioning/pass-319467/assets/mbs/2025/12/11/sample_name/TEST_0002.nxs
    # /nsls2/data3/esm/proposals/commissioning/pass-319467/export/2025_12_11/sample_name/TEST_0002.nxs
    try:
        prefix, suffix = str(fpath_orig).split('assets')
        suffix = suffix.split('/', 2)[-1].replace('/', '_', 2)
        fpath_dest = Path(prefix) / 'export' / suffix
        Path(fpath_dest).parent.mkdir(parents=True, exist_ok=True)

        shutil.copy(fpath_orig, fpath_dest)
        logger.info(f"File '{fpath_orig}' copied successfully to '{fpath_dest}'")
    except Exception as e:
        logger.error(f"An error occurred while copying the file: {e}")

    # Read the metadata from Tiled
    primary = run_client["primary"].read(variables = ['mbs_escale_min', 'mbs_escale_max', 'mbs_num_steps', \
                'mbs_xscale_min', 'mbs_xscale_max', 'mbs_num_slice', \
                'mbs_pass_energy', 'mbs_lens_mode', 'mbs_acq_mode', 'mbs_dith_steps', 'mbs_width', \
                'mbs_center_ke', 'mbs_start_ke', 'mbs_end_ke', 'mbs_step_size', \
                'mbs_act_scans', 'mbs_psu_mode', 'mbs_num_slice', 'mbs_num_steps', 'mbs_frames'])
    baseline = run_client["baseline"].read(variables=['FEslit_h_gap_readback', 'FEslit_v_gap_readback', \
                'EPU105_gap', 'EPU105_phase', 'EPU57_gap', 'EPU57_phase',
                'PGM_Grating_lines', 'PGM_Energy', 'ExitSlitA_h_gap', 'ExitSlitA_v_gap',
                'LT_X', 'LT_Y', 'LT_Z', 'LT_Rx', 'LT_Ry', 'LT_Rz', 'D1', 'D2', 'Stinger'
                ]).tail(1)
    values = {k: v.values[0] for k, v in primary.data_vars.items()} \
        | {k: v.item() for k, v in baseline.data_vars.items()}

    # Add metadata to new (copied) NeXus file
    with nx.nxload(fpath_dest, 'rw') as nxfile:
        nxfile.entry.user=nx.NXuser()
        nxfile.entry.user.name= run_client.start["username"] # nx.NXfield(ui().mbstable.usr_le.text())

        nxfile.entry.instrument.analyzer.loc_name = nx.NXfield('ESM - MBS: L4-054')
        nxfile.entry.instrument.analyzer.energies = np.linspace(values["mbs_escale_min"], values["mbs_escale_max"], values["mbs_num_steps"], endpoint=True)
        nxfile.entry.instrument.analyzer.angles = np.linspace(values["mbs_xscale_min"], values["mbs_xscale_max"], values["mbs_num_slice"], endpoint=True)
        nxfile.entry.instrument.analyzer.lens_mode = nx.NXfield(values["mbs_lens_mode"])
        nxfile.entry.instrument.analyzer.acq_mode = nx.NXfield(values["mbs_acq_mode"])
        nxfile.entry.instrument.analyzer.pass_energy = nx.NXfield(float(values["mbs_pass_energy"]), units='eV')   
        nxfile.entry.instrument.analyzer.dither_steps = nx.NXfield(values["mbs_dith_steps"])
        nxfile.entry.instrument.analyzer.energy_width = nx.NXfield(values["mbs_width"], units='eV')
        nxfile.entry.instrument.analyzer.entrance_slit_direction = nx.NXfield('vertical')
        nxfile.entry.instrument.analyzer.entrance_slit_settings = nx.NXfield(' ')
        nxfile.entry.instrument.analyzer.entrance_slit_shape = nx.NXfield('straight')
        nxfile.entry.instrument.analyzer.entrance_slit_size = nx.NXfield(0, units='mm')
        nxfile.entry.instrument.analyzer.kinetic_energy_center = nx.NXfield(values["mbs_center_ke"], units='eV')
        nxfile.entry.instrument.analyzer.kinetic_energy_start = nx.NXfield(values["mbs_start_ke"], units='eV')
        nxfile.entry.instrument.analyzer.kinetic_energy_end = nx.NXfield(values["mbs_end_ke"], units='eV')
        nxfile.entry.instrument.analyzer.kinetic_energy_step = nx.NXfield(values["mbs_step_size"], units='eV')
        nxfile.entry.instrument.analyzer.number_of_iterations = nx.NXfield(values["mbs_act_scans"])
        nxfile.entry.instrument.analyzer.psu_mode=nx.NXfield(values["mbs_psu_mode"]) 
        nxfile.entry.instrument.analyzer.slices=nx.NXfield(values["mbs_num_slice"])
        nxfile.entry.instrument.analyzer.steps=nx.NXfield(values["mbs_num_steps"])
        nxfile.entry.instrument.analyzer.time_for_frames=nx.NXfield(values["mbs_frames"], units='ms')
        nxfile.entry.instrument.analyzer.local_name=nx.NXfield('ESM - MBSL4-054')

        nxfile.entry.instrument.insertion_device=nx.NXsource()
        nxfile.entry.instrument.insertion_device.name = nx.NXfield('EPU105')
        nxfile.entry.instrument.insertion_device.FEH = nx.NXfield(np.round(values["FEslit_h_gap_readback"],2), units='mm')   # PV:FE:C21A-OP{Slt:12-Ax:X}t2.C
        nxfile.entry.instrument.insertion_device.FEV = nx.NXfield(np.round(values["FEslit_v_gap_readback"],2), units='mm')   # PV:FE:C21A-OP{Slt:12-Ax:Y}t2.C
        nxfile.entry.instrument.insertion_device.gap_105 = nx.NXfield(np.round(values["EPU105_gap"],2), units='mm')   # PV:SR:C21-ID:G1B{EPU:2-Ax:Gap}Mtr.RBV
        nxfile.entry.instrument.insertion_device.phase_105 = nx.NXfield(np.round(values["EPU105_phase"],2), units='mm')   # PV:SR:C21-ID:G1B{EPU:2-Ax:Phase}Mtr.RBV
        nxfile.entry.instrument.insertion_device.gap_57 = nx.NXfield(np.round(values["EPU57_gap"],2), units='mm')   # PV:SR:C21-ID:G1A{EPU:1-Ax:Gap}Mtr.RBV
        nxfile.entry.instrument.insertion_device.phase_57 = nx.NXfield(np.round(values["EPU57_phase"],2), units='mm')   # PV:SR:C21-ID:G1A{EPU:1-Ax:Phase}Mtr.RBV
        
        nxfile.entry.instrument.monochromator=nx.NXmonochromator()
        nxfile.entry.instrument.monochromator.grating=nx.NXfield(values["PGM_Grating_lines"], units='lines/mm')
        nxfile.entry.instrument.monochromator.energy=nx.NXfield(np.round(values["PGM_Energy"],4), units='eV')  # PV:XF:21IDB-OP{Mono:1-Ax:8_Eng}Mtr.RBV
        nxfile.entry.instrument.monochromator.h_gap=nx.NXfield(np.round(values["ExitSlitA_h_gap"],1), units='um')  # PV:XF:21IDC-OP{Slt:1A-Ax:A1_HG}Mtr.RBV
        nxfile.entry.instrument.monochromator.v_gap=nx.NXfield(np.round(values["ExitSlitA_v_gap"],1), units='um')  # PV:XF:21IDC-OP{Slt:1A-Ax:A1_VG}Mtr.RBV

        nxfile.entry.instrument.manipulator=nx.NXpositioner()
        nxfile.entry.instrument.manipulator.type=nx.NXfield('6dof-xyzRxRyRz')
        nxfile.entry.instrument.manipulator.pos_x=nx.NXfield(np.round(values["LT_X"],4),units='mm')  # PV:XF:21IDD-ES{PRV-Ax:X}Mtr.RBV
        nxfile.entry.instrument.manipulator.pos_y=nx.NXfield(np.round(values["LT_Y"],4),units='mm')  # PV:XF:21IDD-ES{PRV-Ax:Y}Mtr.RBV
        nxfile.entry.instrument.manipulator.pos_z=nx.NXfield(np.round(values["LT_Z"],4),units='mm')  # PV:XF:21IDD-ES{PRV-Ax:Z}Mtr.RBV
        nxfile.entry.instrument.manipulator.pos_Rx=nx.NXfield(np.round(values["LT_Rx"],2),units='degree')  # PV:XF:21IDD-ES{PRV-Ax:R3}Mtr.RBV
        nxfile.entry.instrument.manipulator.pos_Ry=nx.NXfield(np.round(values["LT_Ry"],2),units='degree')  # PV:XF:21IDD-ES{PRV-Ax:R1}Mtr.RBV
        nxfile.entry.instrument.manipulator.pos_Rz=nx.NXfield(np.round(values["LT_Rz"],2),units='degree')  # PV:XF:21IDD-ES{PRV-Ax:R2}Mtr.RBV
        nxfile.entry.instrument.manipulator.D1=nx.NXfield(np.round(values["D1"],2),units='K')  # F:21IDD-ES{PS:Heat3}D1_RB
        nxfile.entry.instrument.manipulator.D2=nx.NXfield(np.round(values["D2"],2),units='K')  # XF:21IDD-ES{PS:Heat3}D2_RB
        nxfile.entry.instrument.manipulator.Stinger=nx.NXfield(np.round(values["Stinger"],2),units='K')  # XF:21ID1-ES{TCtrl:2-Chan:A}T-I
        nxfile.entry.instrument.manipulator.sample_bias=nx.NXfield(0, units='V')

        nxfile.entry.note = nx.NXnote()
        nxfile.entry.note.description = nx.NXfield(str(run_client.start.get("user_note", "")))

    elapsed_time = time.monotonic() - start_time
    logger.info(f"Finished exporting metadata; {elapsed_time = }")


@flow(log_prints=True)
def metadata_export_flow(uid):
    export_metadata_task(uid)
