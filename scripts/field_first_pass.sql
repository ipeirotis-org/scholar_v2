-- First-pass field assignment from Google Scholar interests, with per-field norms (2026-11-03).
-- Rough keyword rules, not checked for accuracy. Each interest goes to the first matching rule
-- (pri order); an author's field is the one with the most matching interests, ties to the
-- first-listed interest. See TASKS.md "Field-specific normalization".
-- Run: bq query --use_legacy_sql=false --format=pretty < scripts/field_first_pass.sql
WITH rules AS (
  SELECT * FROM UNNEST([
    STRUCT(1 AS pri, 'Economics & Finance' AS field, r'econom|finance|financial|asset pricing|macro|monetary|labor|industrial organization|trade|public finance|econometric|game theory|mechanism design|auction|banking|market design|political economy|development$|growth|taxation|fiscal' AS re),
    STRUCT(2, 'Business & Management', r'marketing|management|strategy|organization|organizational|entrepreneur|innovation|accounting|operations management|supply chain|consumer|leadership|negotiation|corporate governance|information systems|business|human resource'),
    STRUCT(3, 'Psychology & Cognitive Science', r'psycholog|cognit|emotion|perception|attention|memory|decision making|judgment|social cognition|personality|motivation|language acquisition|psycholinguist|learning sciences|child development|developmental science|autism|psychopath|psychotherapy|well-being|wellbeing'),
    STRUCT(4, 'Neuroscience', r'neuro|brain|synap|electrophysiolog|fmri|neural circuits|vision science|visual system|motor control'),
    STRUCT(5, 'Medicine & Health', r'medicine|medical|clinical|epidemiolog|public health|global health|health|cancer|oncolog|cardio|surgery|surgical|hiv|aids|infectious|vaccine|diabetes|obesity|psychiatr|pediatric|radiolog|nursing|pharmac|disease|patient|nutrition|alzheimer|aging|stroke|trauma|ophthalm|dermat|kidney|nephrol|liver|hepat|transplant|anesthes|imaging|biostatist|outcomes research|health policy|mental health|addiction|substance|tuberculosis|malaria|asthma|sleep'),
    STRUCT(6, 'Life Sciences', r'biolog|genetic|genomic|bioinformatic|immunolog|microbio|evolution|ecology|cell |molecular|protein|biochem|biophysic|structural biology|virolog|plant|stem cell|epigenet|rna|dna|chromatin|developmental|metabolism|physiology|zoology|botany|marine|conservation|population genetics|systems biology|proteomic|crispr|bacteria|parasit|entomolog|animal behavior'),
    STRUCT(7, 'Computer Science', r'machine learning|artificial intelligence|^ai$|computer|computing|deep learning|natural language|nlp|robotic|algorithm|data mining|data science|information retrieval|databases?$|distributed systems|networking|^networks$|security|privacy|cryptograph|programming languages|software|operating systems|hci|human-computer|human computer|graphics|vision$|reinforcement learning|crowdsourcing|recommender|social computing|visualization|theoretical computer science|complexity theory|formal methods|compilers|cloud|wireless|internet|web|multimedia|speech|information theory|computational linguistics|knowledge representation|multi-agent|semantic'),
    STRUCT(8, 'Mathematics & Statistics', r'mathemat|statistic|probabilit|optimization|operations research|stochastic|combinatoric|algebra|geometry|topology|number theory|analysis$|pde|partial differential|dynamical systems|numerical|causal inference|bayesian|applied math|control theory|graph theory|logic'),
    STRUCT(9, 'Physics & Astronomy', r'physics|astro|cosmolog|quantum|condensed matter|particle|optics|photonic|laser|plasma|gravitation|string theory|soft matter|statistical mechanics|superconduct|atomic|nuclear|spintronics|galax|planetary science|exoplanet'),
    STRUCT(10, 'Chemistry & Materials', r'chemi|catalys|materials|nanotech|nanomaterial|polymer|self-assembly|spectroscopy|electrochem|organic|inorganic|crystallograph|biomaterial|nanoscience|surface science|battery|batteries|metal|synthesis|supramolecular|mass spectrometry'),
    STRUCT(11, 'Engineering', r'engineering|mechanic|fluid|electrical|signal processing|control$|controls|microfluidic|mems|energy|power systems|circuits|vlsi|semiconductor|device|sensors?$|biomechanics|tissue engineering|drug delivery|transportation|manufacturing|aerospace|combustion|heat transfer|structural|civil|acoustics|microelectronics|bioengineering|medical devices|biosensor|imaging systems|additive|solar|photovoltaic|turbulence'),
    STRUCT(12, 'Earth & Environmental Sciences', r'climate|geolog|geochem|geophys|ocean|atmospher|earth|hydrolog|environment|sustainab|seismolog|tectonic|paleo|glaciol|remote sensing|meteorolog|biogeochem|water|soil|volcan|mineral|carbon cycle|air pollution|ecosystem'),
    STRUCT(13, 'Social Sciences', r'politic|sociolog|social|international relations|public policy|policy|education|demograph|inequality|gender|race|migration|immigration|criminolog|communication|media|journalism|urban|law|legal|anthropolog|geography|elections|voting|democracy|conflict|terrorism|religion|family|labor markets|poverty|crime|networks? science'),
    STRUCT(14, 'Humanities', r'history|philosoph|literature|linguistics|classics|art|music|theology|ethics|archaeolog|religious studies|film|poetry|rhetoric|medieval|renaissance|cultural studies|epistemolog|metaphysics|aesthetics')
  ])
),
ints AS (
  SELECT scholar_id, LOWER(TRIM(i)) t, off FROM gs_data.authors_latest, UNNEST(interests) i WITH OFFSET off
),
hits AS (
  -- each interest goes to the first matching rule in priority order
  SELECT scholar_id, off, ARRAY_AGG(field ORDER BY pri LIMIT 1)[OFFSET(0)] field
  FROM ints JOIN rules ON REGEXP_CONTAINS(t, re) GROUP BY scholar_id, off
),
votes AS (
  SELECT scholar_id, field, COUNT(*) n, MIN(off) first_off FROM hits GROUP BY 1,2
),
pick AS (
  SELECT scholar_id, ARRAY_AGG(STRUCT(field, n) ORDER BY n DESC, first_off LIMIT 1)[OFFSET(0)].field field
  FROM votes GROUP BY 1
),
j AS (
  SELECT IFNULL(p.field, '(unassigned)') field, s.total_publications pubs, s.citedby, s.hindex,
         GREATEST(2026 - s.year_of_first_pub, 1) career_yrs, r.pip_auc_score_percentile pip_pct
  FROM gs_statistics.stats_author_current s
  LEFT JOIN pick p USING (scholar_id)
  LEFT JOIN gs_statistics.ranked_author_pip_scores_current r USING (scholar_id)
)
SELECT field, COUNT(*) authors,
  APPROX_QUANTILES(career_yrs, 2)[OFFSET(1)] med_career_yrs,
  APPROX_QUANTILES(pubs, 2)[OFFSET(1)] med_pubs,
  ROUND(APPROX_QUANTILES(pubs / career_yrs, 2)[OFFSET(1)], 1) med_pubs_per_yr,
  APPROX_QUANTILES(citedby, 2)[OFFSET(1)] med_cites,
  ROUND(APPROX_QUANTILES(citedby / NULLIF(pubs,0), 2)[OFFSET(1)], 0) med_cites_per_pub,
  APPROX_QUANTILES(hindex, 2)[OFFSET(1)] med_h,
  ROUND(APPROX_QUANTILES(pip_pct, 2)[OFFSET(1)], 2) med_pip_pct
FROM j GROUP BY 1 ORDER BY med_pubs_per_yr DESC
