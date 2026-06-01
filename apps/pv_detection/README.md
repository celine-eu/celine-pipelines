# PV Detection Pipeline

Detects existing photovoltaic (PV) panel installations on building rooftops from aerial orthophoto imagery using vision language models.

Part of the [CELINE](https://github.com/celine-eu) platform. Complements `pv_estimation` (which determines PV **suitability**) by identifying which buildings **already have PV installed**.

## Detection Approach

The pipeline is **building-driven**: it queries known building footprints from `pv_overture_buildings`, fetches the corresponding aerial tile, crops each building's rooftop, and classifies it via a vision model.

```
pv_overture_buildings (Overture Maps, all Trentino)
       |
       v
Tile Provider (filesystem or WMS)
  - Fetch aerial tile covering each building's bbox
  - Crop rooftop using building polygon bounds
  - Skip edge crops (<50% of building visible in tile)
       |
       v
Vision Model Detector (Ollama)
  - Qwen2.5-VL 7B or similar vision-language model
  - Prompt-based binary classification: has PV panels or not
  - Outputs: has_pv (bool), confidence (0-1), reasoning (text)
       |
       v
raw.pv_predictions → dbt staging → silver → gold
  - Silver: deduplicated per building (latest detection wins)
  - Gold: joined with building geometry, GIST-indexed, GeoJSON
```

### Model configuration

The detector is abstracted behind a `Detector` interface. The default `OllamaDetector` is configurable in `flows/config.yaml`:

- **Model**: any Ollama vision model (`qwen2.5vl:7b`, `llava:7b`, `minicpm-v:8b`, etc.)
- **Host**: local or remote Ollama instance
- **Prompt**: customizable classification prompt
- **Threshold**: confidence cutoff for positive classification

To swap models, change `detector.ollama.model` in config. To point to a remote GPU server, change `detector.ollama.host`. To add a completely different backend (API-based, PANEL CLIP, YOLO), implement a new `Detector` subclass.

### Tile providers

- **FilesystemProvider**: loads pre-downloaded tiles from `data/tiles/`. Each tile has a `.jpg` image and `.jpg.json` metadata sidecar with EPSG:25832 bbox and WGS84 centroid.
- **WmsProvider**: (planned) fetches tiles from WMS endpoint centered on each building.

### Phase 2 (planned)

Detection results from Phase 1 bootstrap a labeled dataset for fine-tuning YOLOv8, which replaces the LLM for faster production inference.

## Quickstart

```bash
cd apps/pv_detection

# Install deps, start Ollama, pull vision model
task setup

# Download reference sample tiles
task download:samples

# Launch the Streamlit viewer (browse tiles, overlay buildings, run detection)
task viewer
```

## Aerial Imagery & AGEA Copyright

### Ownership model

All Italian aerial orthophotos at 20cm resolution are produced by **AGEA** (Agenzia per le Erogazioni in Agricoltura), which retains full copyright. AGEA grants usage rights to Regions and Autonomous Provinces through **triennial conventions** (*Convenzione per la concessione della licenza d'uso dei prodotti aerofotogrammetrici*). Each region then sub-licenses to local bodies under its own terms.

> *"Le ortofoto digitali sono di proprietà dell'Agenzia per le Erogazioni in Agricoltura (AGEA), pertanto l'uso deve sottostare al relativo copyright."*

### Per-region licensing

The downstream license terms **vary significantly by region**. The same AGEA 20cm dataset is available under different conditions depending on who publishes it:

| Region / Province | License | Commercial use | Derivatives / ML | Download | Reference |
|---|---|---|---|---|---|
| **Provincia Autonoma di Trento** | Institutional use only | Prohibited | Prohibited | Prohibited | [Condizioni AGEA 2023](https://siatservices.provincia.tn.it/idt/allegati/ORTO/Sintesi_delle_condizioni_di_utilizzo_AGEA2023.pdf) |
| **Provincia Autonoma di Bolzano** | **CC BY 4.0** | **Allowed** | **Allowed** | **Allowed** | [Ortofoto 2023 Alto Adige](https://geonetwork1.civis.bz.it/geonetwork/srv/api/records/p_bz:Orthoimagery:Aerial-2023-RGB) |
| Emilia-Romagna | CC BY-NC-ND 3.0 | Prohibited | Prohibited | WMS only | [Geoportale ER](https://geoportale.regione.emilia-romagna.it/approfondimenti/licenze-di-utilizzo-dei-dati) |
| Sicilia | CC BY-NC-ND 3.0 IT | Prohibited | Prohibited | WMS only | [SITR Sicilia](https://www.sitr.regione.sicilia.it/geoportale/it/metadata/details/598) |
| Veneto | Visualization only | Prohibited | Prohibited | Prohibited | [IDT Veneto](https://idt2.regione.veneto.it/condizioni_utilizzo_geoportale/) |
| Geoportale Nazionale (PCN) | CC BY-NC-ND 3.0 IT | Prohibited | Prohibited | WMS only | [PCN](http://www.pcn.minambiente.it/) |

### Implications for this pipeline

**For development and training**: use the **South Tyrol (Bolzano) CC BY 4.0** dataset. Same 20cm AGEA imagery, same building types, fully open for ML training, derivatives, and redistribution with attribution.

```
WMS:   https://geoservices.buergernetz.bz.it/mapproxy/ows
Layer: p_bz-Orthoimagery:Aerial-2023-RGB
CRS:   EPSG:25832
```

**For Trentino production**: requires formal authorization through the AGEA convention chain

1. **FBK** (Fondazione Bruno Kessler) qualifies as *Ente Strumentale* of the Provincia Autonoma di Trento
2. FBK contacts the Provincia (SIAT / Servizio Catasto) to formalize access
3. **SPXL** (Spindox, CELINE coordinator) requests access through FBK for the declared CELINE project scope
4. Scope: PV installation mapping for energy community planning — institutional research activity, not commercial use

### Attribution

When using AGEA imagery, all outputs must carry:

> Ortofoto 20 cm © 2023 AGEA - Agenzia per le Erogazioni in Agricoltura, Roma (www.agea.gov.it) - TUTTI I DIRITTI RISERVATI

When using South Tyrol CC BY 4.0 imagery:

> Ortofoto 2023 - Provincia Autonoma di Bolzano - CC BY 4.0

## Project Structure

```
apps/pv_detection/
├── flows/
│   ├── pipeline.py          # Prefect flow
│   ├── providers.py         # Tile providers (filesystem, WMS stub)
│   ├── detector.py          # Vision model detector (Ollama)
│   └── config.yaml          # Pipeline configuration
├── tools/
│   ├── viewer.py            # Streamlit tile viewer + detection UI
│   ├── download_tiles.py    # WMS tile downloader CLI
│   └── requirements.txt     # Python dependencies
├── dbt/
│   └── models/              # staging → silver → gold
├── data/
│   └── tiles/               # Downloaded tiles + metadata + detection cache
├── docker-compose.yaml      # Ollama with GPU
├── taskfile.yaml             # Task runner commands
└── governance.yaml           # Dataset catalog
```

## Task Commands

| Command | Description |
|---------|-------------|
| `task setup` | Install deps + start Ollama + pull model |
| `task ollama:up` | Start Ollama container |
| `task ollama:pull` | Pull/update the vision model |
| `task viewer` | Launch Streamlit tile viewer |
| `task download:samples` | Download reference sample tiles |
| `task download -- [args]` | Download tiles by coords or from DB |
| `task detect` | Run detection pipeline (Prefect) |
| `task dbt:run` | Run dbt models |
| `task dbt:test` | Run dbt tests |
