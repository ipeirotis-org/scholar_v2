CREATE OR REPLACE VIEW `scholar-version2.gs_statistics.author_interest_fields` AS
-- Level 2: each profiled author's field group from their Google Scholar interests.
--
-- 11 groups, each a union of Semantic Scholar fieldsOfStudy labels (see TASKS.md
-- "Field-specific normalization"). Neuroscience sits with Biology and Business
-- with Economics because their publication norms match; Humanities sits with
-- Social Sciences because it is too small to rank on its own.
--
-- 1. Each interest label maps to the group of its first matching rule (lowest
--    priority number). Priority 0 is a list of common cross-field labels with a
--    fixed answer; later rules are ordered so that the more specific vocabulary
--    wins (e.g. "health economics" -> Economics, "medical imaging" -> Medicine,
--    "computational biology" -> Biology), and methods vocabulary (Mathematics &
--    Statistics: "optimization", "bayesian") loses to any domain word ("clean
--    energy optimization" -> Engineering).
-- 2. An author's group is the one most of their interests map to; ties go to the
--    first-listed interest (GS shows interests in the author's order).
-- Authors whose interests match no rule get no row; author_coauthor_fields
-- covers some of them. Checked 2026-11-03 on a random sample of 80 authors:
-- 3 wrong, all fixed by these rules; ties between two fields stay a judgment call.
-- Cheap for one author (the cache layer filters by scholar_id), so new crawls
-- get a field without waiting for a materialize run.
WITH
  Rules AS (
    SELECT * FROM UNNEST([
      STRUCT(0 AS priority, 'Physics & Astronomy' AS field_group,
             r'^(statistical (mechanics|physics)|biological physics|planetary science|quantum (information|optics|matter)|thermodynamics|nonlinear dynamics)$' AS pattern),
      STRUCT(0, 'Chemistry & Materials',
             r'^(quantum chemistry|molecular dynamics|molecular simulations?|drug discovery|medicinal chemistry|computational chemistry|supramolecular chemistry)$'),
      STRUCT(0, 'Engineering',
             r'^(drug delivery|biomedical engineering|control theory|chemical engineering|neural engineering|neuroengineering|medical devices|statistical signal processing|mechanics of materials|optoelectronics|nanofabrication|communications|cognitive radio)$'),
      STRUCT(0, 'Medicine & Health',
             r'^(biostatistics|environmental health|neurology|neurosurgery|neuro-oncology|neuroradiology|neuropathology|clinical neuroscience|global health|health services research|rehabilitation|drug development)$'),
      STRUCT(0, 'Psychology & Cognitive Science',
             r'^(cognitive (development|neuroscience|science|modeling)|social neuroscience|decision[- ]making|judgment and decision making|language acquisition|psycholinguistics|intergroup relations|individual differences)$'),
      STRUCT(0, 'Biology & Neuroscience',
             r'^(computational biology|bioinformatics|mathematical biology|statistical genetics|chemical biology|biochemistry|biophysics|structural biology|neuroimaging|fmri|computational neuroscience|plant pathology|biogeography|eeg|dopamine|transcription|symbiosis|cytoskeleton|systematics|agriculture)$'),
      STRUCT(0, 'Mathematics & Statistics',
             r'^(operations research|causal inference|applied probability|uncertainty quantification|inverse problems|computational geometry|time series|mathematical physics)$'),
      STRUCT(0, 'Computer Science',
             r'^(information theory|computational linguistics|algorithmic game theory|data management|computational (photography|imaging)|quantum computing|complex networks|human-robot interaction|human-ai interaction|cscw|program synthesis|haptics|interaction design|text mining|data analytics|informatics)$'),
      STRUCT(0, 'Social Sciences & Humanities',
             r'^(network science|science of science|computational social science|public opinion|misinformation|economic sociology|bioethics|syntax|semantics|phonology|prosody|pragmatics|digital humanities|epistemology|sexuality|violence|international security|diversity)$'),
      STRUCT(0, 'Economics & Business',
             r'^(information systems|operations management|market design|mechanism design|game theory|law and economics|pricing|real estate|venture capital|antitrust|management|decision theory)$'),

      STRUCT(0, 'Earth & Environmental Sciences', r'^(geochronology|global change)$'),

      STRUCT(1, 'Earth & Environmental Sciences',
             r'geolog|geochem|geophys|geomorph|geobiol|biogeochem|\bocean|atmospher|\bearth\b|hydrolog|seismolog|tectonic|\bpaleo|glaciol|remote sensing|meteorolog|\bclimate\b|carbon cycle|air (pollution|quality)|\bsoils?\b|volcan|mineralog|petrolog|sediment|water (resources|quality)|environmental (science|sciences|engineering|chemistry)|^environment$|sustainab|\becosystems?\b|land use|wildfire|natural hazards|coastal|limnolog|\bgis\b'),
      STRUCT(2, 'Economics & Business',
             r'\becon|macroecon|microecon|financ|asset pricing|monetary|labou?r (economics|markets?)|industrial organi[sz]ation|\btrade\b|accounting|marketing|entrepreneur|\bstrategy\b|strategic management|organi[sz]ational (behavio|theory|economics)|organization theory|^organizations$|corporate (governance|strategy)|consumer behavio|negotiation|leadership|supply chain|management science|^innovation$|innovation (management|policy|economics)|\bbusiness|human resource|\bauctions?\b|political economy|taxation|fiscal|^development$|\bbanking\b'),
      STRUCT(3, 'Psychology & Cognitive Science',
             r'psycholog|\bcognit|emotion|perception|\battention\b|\bmemory\b|judgment|personality|motivation|child development|autism|well-?being|creativity|self-regulation|social cognition|prejudice|stereotyp|happiness|moral (psychology|judgment|cognition)|learning sciences|psychometric|\bcognition\b'),
      STRUCT(4, 'Medicine & Health',
             r'medicine|medical|clinical|epidemiolog|public health|\bhealth\b|healthcare|cancer|oncolog|tumou?r|cardi(o|ac)|surg(ery|ical)|\bhiv\b|\baids\b|infectious|vaccin|diabet|obesity|psychiatr|pediatric|paediatric|radiolog|nursing|pharmac|disease|patient|nutrition|alzheimer|parkinson|\baging\b|ageing|stroke|trauma|ophthalm|dermatol|kidney|nephrol|\bliver\b|hepat|transplant|anesthes|anaesthes|outcomes research|addiction|substance (use|abuse)|tuberculosis|malaria|asthma|\bsleep\b|depression|anxiety|schizophren|psychosis|\bptsd\b|suicid|epilep|\bpain\b|patholog|immunotherap|gene therapy|biomarker|\bmri\b|magnetic resonance|imaging|prevention|implementation science|neurolog|neurosurg|gastro|endocrin|rheumat|urolog|gynecol|obstetric|maternal|reproductive|injury|hematolog|lymphoma|leukemia|myeloma|inflammation|sepsis|critical care|emergency|orthop|dental|dentistry|hearing|audiolog|otolaryng|covid|influenza|antimicrobial|clinical trials|nanomedicine'),
      STRUCT(5, 'Biology & Neuroscience',
             r'biolog|genetic|genomic|bioinformatic|immunolog|immunity|microbio|evolution|ecolog|\bcells?\b|molecular|protein|biochem|biophysic|virolog|\bplants?\b|stem cell|epigenet|\brna\b|\bdna\b|chromatin|developmental|metabolism|physiolog|zoolog|botan|marine|conservation|phylogen|proteomic|metabolomic|crispr|bacteri|parasit|entomolog|animal behavio|neuro|\bbrain\b|synap|electrophysiol|neural circuits|vision science|visual (system|neuroscience)|motor control|gene (regulation|expression)|signal(l?ing| transduction)|mitochond|ion channels?|biotechnology|cryo-?em|cryo-electron|ornitholog|biodiversity|wildlife|microbiome|mechanobiology|\bbiology\b'),
      STRUCT(6, 'Computer Science',
             r'machine learning|artificial intelligence|\bai\b|computer|computing|deep learning|natural language|\bnlp\b|robot|algorithm|data mining|data science|big data|information retrieval|\bdatabases?\b|distributed systems|networking|^networks?$|^security$|(computer|network|information|software|systems|hardware|cyber|web|mobile) security|privacy|cryptograph|programming languages|software|operating systems|\bhci\b|human[- ]computer|graphics|^vision$|reinforcement learning|crowdsourcing|recommender|social computing|visualization|complexity theory|formal (methods|verification)|compilers?|\bcloud\b|wireless|internet|\bweb\b|multimedia|speech|knowledge representation|multi-?agent|semantic web|image (processing|analysis)|pattern recognition|augmented reality|virtual reality|accessibility|\bcyber|embedded systems|blockchain|\bllms?\b|large language models|neural networks|computer architecture'),
      STRUCT(7, 'Physics & Astronomy',
             r'physics|\bastro|cosmolog|quantum|condensed matter|particle physics|high energy|optics|photonic|plasmonic|\blasers?\b|plasma|gravitation|general relativity|string theory|soft matter|superconduct|\batomic\b|nuclear physics|spintronic|galax|exoplanet|magnetism|ultrafast|metamaterial|cold atoms|astronomy|dark (energy|matter)'),
      STRUCT(8, 'Chemistry & Materials',
             r'chemi(stry|cal)|catalys|materials|nanotech|nanomaterial|nanoparticle|nanoscience|polymer|self-assembly|spectroscop|electrochem|\borganic\b|inorganic|crystallograph|biomaterial|surface science|batter(y|ies)|\bmetals?\b|synthesis|supramolecular|mass spectrometry|thin films|2d materials|graphene|perovskite|metal-organic|colloid|zeolite|photochem'),
      STRUCT(9, 'Engineering',
             r'engineering|mechanic(al|s)|\bfluids?\b|fluid dynamics|electrical|signal processing|\bcontrols?\b|control systems|microfluidic|\bmems\b|energy|power (systems|electronics)|circuits|\bvlsi\b|semiconductor|\bdevices?\b|\bsensors?\b|biosensor|biomechanic|transportation|manufacturing|aerospace|combustion|heat transfer|structural (dynamics|health)|\bcivil\b|acoustics|microelectronics|nanoelectronics|bioengineering|solar|photovoltaic|turbulence|aerodynamic|smart grid|antenna|\bradar\b|reliability|tribology|finite element'),
      STRUCT(10, 'Mathematics & Statistics',
             r'mathemat|statistic|probabilit|optimi[sz]ation|stochastic|combinatoric|algebra|geometry|topology|number theory|(functional|harmonic|numerical|real|complex|mathematical|statistical|survival|bayesian) analysis|\bpdes?\b|partial differential|dynamical systems|numerical|bayesian|graph theory|\blogic\b|nonparametric|high-dimensional|experimental design'),
      STRUCT(11, 'Social Sciences & Humanities',
             r'politic|sociolog|\bsocial\b|international relations|policy|education|demograph|inequality|gender|\brace\b|racism|migration|immigra|criminolog|communication|\bmedia\b|journalism|urban|\blaw\b|legal|anthropolog|geography|elections|voting|democracy|conflict|terrorism|religio|\bfamily\b|poverty|\bcrime\b|history|philosoph|literature|linguistic|classics|\bart\b|art history|music|theology|ethics|archaeolog|film studies|poetry|rhetoric|medieval|renaissance|cultural|\bculture\b|human rights|intellectual property|area studies|latin america|\bafrica|\basia|\bchina\b|\bjapan|\bstudies\b')
    ])
  ),
  InterestGroups AS (
    SELECT a.scholar_id, pos, ARRAY_AGG(r.field_group ORDER BY r.priority LIMIT 1)[OFFSET(0)] AS field_group
    FROM `scholar-version2.gs_data.authors_latest` a, UNNEST(a.interests) AS interest WITH OFFSET AS pos
    JOIN Rules r ON REGEXP_CONTAINS(LOWER(TRIM(interest)), r.pattern)
    GROUP BY a.scholar_id, pos
  )
SELECT scholar_id,
       ARRAY_AGG(field_group ORDER BY votes DESC, first_pos LIMIT 1)[OFFSET(0)] AS field_group
FROM (
  SELECT scholar_id, field_group, COUNT(*) AS votes, MIN(pos) AS first_pos
  FROM InterestGroups
  GROUP BY scholar_id, field_group
)
GROUP BY scholar_id;
