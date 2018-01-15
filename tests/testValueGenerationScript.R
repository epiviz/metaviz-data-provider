require(metagenomeSeq)
require(msd16s)
require(vegan)

normed_msd16s <- cumNorm(msd16s, p=.75)
subset_samples <- c("100259","100262","100267")
subset_msd16s <- normed_msd16s[,subset_samples]

aggregated_subset <- aggregateByTaxonomy(obj = subset_msd16s, lvl = "phylum", norm = TRUE)
aggregated_subset_counts <- MRcounts(aggregated_subset)

alpha_diversity_measures <- diversity(t(aggregated_subset_counts))
