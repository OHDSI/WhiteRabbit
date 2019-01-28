package org.ohdsi.rabbitInAHat;

import javax.swing.JTextArea;
import javax.swing.UIManager;

public class DescriptionTextArea extends JTextArea {

	private static final long	serialVersionUID	= -3065135241375027552L;

	public DescriptionTextArea(String text) {
		super(text);

		setWrapStyleWord(true);
		setLineWrap(true);
		setOpaque(false);
		setEditable(false);
		setFocusable(false);
		setBackground(UIManager.getColor("Label.background"));
		setFont(UIManager.getFont("Label.font"));
		setBorder(UIManager.getBorder("Label.border"));
	}

}
