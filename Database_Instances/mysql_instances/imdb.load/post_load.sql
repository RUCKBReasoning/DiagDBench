ALTER TABLE `aka_name` ADD KEY `person_id_aka_name` (`person_id`);
ALTER TABLE `aka_title`
  ADD KEY `kind_id_aka_title` (`kind_id`),
  ADD KEY `movie_id_aka_title` (`movie_id`);
ALTER TABLE `cast_info`
  ADD KEY `movie_id_cast_info` (`movie_id`),
  ADD KEY `person_id_cast_info` (`person_id`),
  ADD KEY `person_role_id_cast_info` (`person_role_id`),
  ADD KEY `role_id_cast_info` (`role_id`);
ALTER TABLE `complete_cast` ADD KEY `movie_id_complete_cast` (`movie_id`);
ALTER TABLE `movie_companies`
  ADD KEY `company_id_movie_companies` (`company_id`),
  ADD KEY `company_type_id_movie_companies` (`company_type_id`),
  ADD KEY `movie_id_movie_companies` (`movie_id`);
ALTER TABLE `movie_info`
  ADD KEY `info_type_id_movie_info` (`info_type_id`),
  ADD KEY `movie_id_movie_info` (`movie_id`);
ALTER TABLE `movie_info_idx`
  ADD KEY `info_type_id_movie_info_index` (`info_type_id`),
  ADD KEY `movie_id_movie_info_index` (`movie_id`);
ALTER TABLE `movie_keyword`
  ADD KEY `keyword_id_movie_keyword` (`keyword_id`),
  ADD KEY `movie_id_movie_keyword` (`movie_id`);
ALTER TABLE `movie_link`
  ADD KEY `link_type_id_movie_link` (`link_type_id`),
  ADD KEY `linked_movie_id_movie_link` (`linked_movie_id`),
  ADD KEY `movie_id_movie_link` (`movie_id`);
ALTER TABLE `person_info`
  ADD KEY `info_type_id_person_info` (`info_type_id`),
  ADD KEY `person_id_person_info` (`person_id`);
ALTER TABLE `title` ADD KEY `kind_id_title` (`kind_id`);
