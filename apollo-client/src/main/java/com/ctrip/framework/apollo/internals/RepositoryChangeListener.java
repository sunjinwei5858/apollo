/*
 * Copyright 2021 Apollo Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ctrip.framework.apollo.internals;

import java.util.Properties;

/**
 *3:RepositoryChangeListener、ConfigChangeListener负责处理配置发布事件
 *
 * 二者主要做的事情是
 * 1:过滤出修改过的配置
 * 2:更新修改配置的bean,
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public interface RepositoryChangeListener {
  /**
   * Invoked when config repository changes.
   * @param namespace the namespace of this repository change
   * @param newProperties the properties after change
   */
  void onRepositoryChange(String namespace, Properties newProperties);
}
