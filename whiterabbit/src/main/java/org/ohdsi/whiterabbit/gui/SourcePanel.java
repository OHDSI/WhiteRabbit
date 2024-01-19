/*******************************************************************************
 * Copyright 2023 Observational Health Data Sciences and Informatics & The Hyve
 *
 * This file is part of WhiteRabbit
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.ohdsi.whiterabbit.gui;

import javax.swing.*;
import java.util.*;

public class SourcePanel extends JPanel {
    private List<JComponent> clearableComponents = new ArrayList<>();

    public void addReplacable(JComponent component) {

        this.add(component);
        clearableComponents.add(component);
    }

    public void clear() {
        // remove the components in the reverse order of how they were added, keeps the layout of the JPanel intact
        Collections.reverse(clearableComponents);
        clearableComponents.forEach(this::remove);
        clearableComponents.clear();
    }

}
