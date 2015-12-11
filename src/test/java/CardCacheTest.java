import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import db.DBConnectionManager;
import org.apache.commons.httpclient.util.URIUtil;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CardCacheTest {

  private final static int blockSize = 1000;
  private final static String allCardsUri = "http://api.mtgapi.com/v2/cards?page=";
  private final static String cardByNameUri = "http://api.mtgapi.com/v1/card/name/";
  private final static String cardByIdUri = "http://api.mtgapi.com/v1/card/id/";
  private final int NO_CARD_ID = -1;

  private Connection conn;

  @Before
  public void init() throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
    System.out.println("Started at: " + LocalDateTime.now());
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    conn = DriverManager.getConnection("jdbc:mysql://localhost/mtg?user=root&password=Qwer812$");
  }

  @After
  public void teardown() throws SQLException {
    System.out.println("Finished at: " + LocalDateTime.now());
    conn.close();
  }

  @Test
  public void testInsert() throws Exception {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    final Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/mtg?" +
            "user=root&password=Qwer812$");


    final Statement stmt = conn.createStatement();
    final boolean result = stmt.execute("INSERT INTO cd_card (cd_multiverse_id, cd_name) VALUE (15, 'gal')");
    System.out.println(result);
  }

  @Test
  @Ignore
  public void testMultiverseIdComplition() throws Exception {
    final Set<String> cardNamesWithoutMultiverseId = Sets.newHashSet("1996 World Champion", "Abhorrent Overlord", "Abyssal Specter", "Abyssal Specter", "Abzan Ascendancy", "Abzan Beastmaster", "Accumulated Knowledge", "Acidic Slime", "Acquire", "Act of Treason", "Aeronaut Tinkerer", "Aesthir Glider", "Air Elemental", "Air Elemental", "Air Elemental", "Ajani Goldmane", "Ajani Steadfast", "Ajani Vengeant", "Ajani Vengeant", "Ajani", "Caller of the Pride", "Alabaster Potion", "Alabaster Potion", "Albino Troll", "All Is Dust", "Allosaurus Rider", "Anafenza", "the Foremost", "Anathemancer", "Ancestral Recall", "Ancestral Recall", "Ancient Grudge", "Ancient Hellkite", "Ancient Ziggurat", "Angel of Glory's Rise", "Angelic Skirmisher", "Animate Artifact", "Animate Artifact", "Animate Dead", "Animate Dead", "Animate Wall", "Animate Wall", "Ankh of Mishra", "Ankh of Mishra", "Ankle Shanker", "Ankle Shanker", "Ant Queen", "Anthousa", "Setessan Hero", "Arashin Sovereign", "Arbiter of the Ideal", "Arc Lightning", "Archdemon of Greed", "Archfiend of Depravity", "Archon of the Triumvirate", "Arcum's Weathervane", "Argothian Enchantress", "Armadillo Cloak", "Armageddon", "Armageddon", "Armageddon", "Armageddon", "Armored Pegasus", "Armored Pegasus", "Arrest", "Arrogant Wurm", "Artisan of Kozilek", "Ascendant Evincar", "Ashen Ghoul", "Ashnod's Coupon", "Aspect of Wolf", "Aspect of Wolf", "Ass Whuppin'", "Astral Slide", "Aura of Silence", "Auramancer", "Aurochs", "Avacyn's Pilgrim", "Avalanche Riders", "Avalanche Tusker", "Avalanche Tusker", "Avatar of Discord", "Avatar of Hope", "Avatar of Woe", "Azorius Guildmage", "Bad Moon", "Bad Moon", "Badlands", "Badlands", "Balance", "Balance", "Balance", "Balduvian Bears", "Balduvian Dead", "Balduvian Horde", "Balduvian Horde", "Ball Lightning", "Banefire", "Banisher Priest", "Banishing Light", "Barbed Sextant", "Barbed Sextant", "Basalt Monolith", "Basalt Monolith", "Basking Rootwalla", "Battering Ram", "Battering Ram", "Batterskull", "Bayou", "Bayou", "Beast of Burden", "Benalish Hero", "Benalish Hero", "Benalish Knight", "Berserk", "Berserk", "Bident of Thassa", "Bile Blight", "Binding Grasp", "Birds of Paradise", "Birds of Paradise", "Birds of Paradise", "Bitterblossom", "Bituminous Blast", "Black Knight", "Black Knight", "Black Knight", "Black Knight", "Black Lotus", "Black Lotus", "Black Sun's Zenith", "Black Vise", "Black Vise", "Black Ward", "Black Ward", "Blanchwood Armor", "Blastoderm", "Blaze", "Blaze of Glory", "Blaze of Glory", "Blessing", "Blessing", "Blightning", "Blood Knight", "Bloodbraid Elf", "Bloodcrazed Neonate", "Bloodlord of Vaasgoth", "Bloodmark Mentor", "Bloodsoaked Champion", "Bloodstained Mire", "Bloodthrone Vampire", "Blue Elemental Blast", "Blue Elemental Blast", "Blue Elemental Blast", "Blue Ward", "Blue Ward", "Bog Imp", "Bog Imp", "Bog Wraith", "Bog Wraith", "Bog Wraith", "Bog Wraith", "Boggart Ram-Gang", "Boltwing Marauder", "Bonescythe Sliver", "Bonesplitter", "Boneyard Wurm", "Boomerang", "Boomerang", "Booster Tutor", "Bottle Gnomes", "Bounty of the Hunt", "Bounty of the Hunt", "Brain Maggot", "Braingeyser", "Braingeyser", "Brainstorm", "Brainstorm", "Bramblewood Paragon", "Brave the Elements", "Breaking", "Breath of Malfegor", "Bribery", "Brion Stoutarm", "Broodmate Dragon", "Browbeat", "Browse", "Brushland", "Budoka Pupil", "Bull Elephant", "Bull Hippo", "Burning Wish", "Burrowing", "Burrowing", "Burst Lightning", "Butcher of the Horde", "Cabal Coffers", "Cabal Therapy", "Calciderm", "Call of the Conclave", "Call of the Herd", "Camouflage", "Camouflage", "Cancel", "Cancel", "Canopy Spider", "Capsize", "Carnival Hellsteed", "Carnivorous Plant", "Carnophage", "Carrion Feeder", "Castigate", "Casting of Bones", "Castle", "Castle", "Cathedral of War", "Celestial Archon", "Celestial Colonnade", "Celestial Prism", "Celestial Prism", "Celestial Purge", "Cenn's Tactician", "Chainer's Edict", "Chandra", "Pyromaster", "Chandra", "Pyromaster", "Chandra's Fury", "Chandra's Phoenix", "Channel", "Channel", "Chaos Orb", "Chaos Orb", "Chaoslace", "Chaoslace", "Char", "Chief Engineer", "Chill", "Chrome Mox", "Cinder Pyromancer", "Circle of Flame", "Circle of Protection: Art", "Circle of Protection: Black", "Circle of Protection: Black", "Circle of Protection: Black", "Circle of Protection: Black", "Circle of Protection: Blue", "Circle of Protection: Blue", "Circle of Protection: Green", "Circle of Protection: Green", "Circle of Protection: Red", "Circle of Protection: Red", "Circle of Protection: Red", "Circle of Protection: Red", "Circle of Protection: Red", "Circle of Protection: White", "Circle of Protection: White", "Circular Logic", "City of Brass", "Civic Wayfinder", "Clockwork Beast", "Clockwork Beast", "Clockwork Beast", "Clockwork Beast", "Clone", "Clone", "Cloud Pirates", "Cloud Sprite", "Cloudpost", "Coat of Arms", "Cockatrice", "Cockatrice", "Coiling Oracle", "Colossal Whale", "Combat Medic", "Comet Storm", "Command Tower", "Condemn", "Consecrate Land", "Consecrate Land", "Conservator", "Conservator", "Consume Spirit", "Consume Spirit", "Consuming Aberration", "Contagion", "Contagion Clasp", "Contract from Below", "Contract from Below", "Control Magic", "Control Magic", "Conversion", "Conversion", "Copper Tablet", "Copper Tablet", "Copy Artifact", "Copy Artifact", "Corpsejack Menace", "Corrupt", "Corrupt", "Counterbore", "Counterspell", "Counterspell", "Counterspell", "Counterspell", "Counterspell", "Courser of Kruphix", "Crackling Doom", "Crater's Claws", "Craw Wurm", "Craw Wurm", "Creature Bond", "Creature Bond", "Creeping Mold", "Crowd of Cinders", "Crucible of Worlds", "Cruel Edict", "Crusade", "Crusade", "Crusade", "Cryptborn Horror", "Cryptic Command", "Crystal Rod", "Crystal Rod", "Crystalline Sliver", "Cultivate", "Cunning Wish", "Cuombajj Witches", "Curse of the Bloody Tome", "Curse of Thirst", "Curse of Wizardry", "Cursed Land", "Cursed Land", "Cursed Land", "Cursed Land", "Cyclopean Tomb", "Cyclopean Tomb", "Damnation", "Dark Banishing", "Dark Banishing", "Dark Confidant", "Dark Privilege", "Dark Ritual", "Dark Ritual", "Dark Ritual", "Dark Ritual", "Dark Ritual", "Dark Ritual", "Dark Ritual", "Darkpact", "Darkpact", "Darksteel Ingot", "Dauntless Dourbark", "Dauthi Slayer", "Dawnbringer Charioteers", "Day of Judgment", "Day of Judgment", "Deadbridge Goliath", "Deadly Insect", "Death", "Death Spark", "Death Spark", "Death Ward", "Death Ward", "Deathbringer Regent", "Deathgrip", "Deathgrip", "Deathlace", "Deathlace", "Deathless Angel", "Decree of Justice", "Deep Analysis", "Deflecting Palm", "Deluge", "Demigod of Revenge", "Demon's Horn", "Demonic Attorney", "Demonic Attorney", "Demonic Hordes", "Demonic Hordes", "Demonic Tutor", "Demonic Tutor", "Demonic Tutor", "Denizen of the Deep", "Deranged Hermit", "Desert", "Despise", "Detonate", "Detonate", "Devil's Play", "Diabolic Edict", "Dictate of Kruphix", "Dictate of the Twin Gods", "Dig Through Time", "Dimir Charm", "Dimir Guildmage", "Dingus Egg", "Dingus Egg", "Diregraf Ghoul", "Dirtcowl Wurm", "Disdainful Stroke", "Disenchant", "Disenchant", "Disenchant", "Disenchant", "Disenchant", "Disenchant", "Disenchant", "Disintegrate", "Disintegrate", "Disintegrate", "Disintegrate", "Dismember", "Dismiss", "Disrupting Scepter", "Disrupting Scepter", "Dissipate", "Dissolve", "Djinn Illuminatus", "Doom Blade", "Doomwake Giant", "Door of Destinies", "Doran", "the Siege Tower", "Doubling Season", "Dragon Broodmother", "Dragon Fodder", "Dragon Throne of Tarkir", "Dragon Whelp", "Dragon Whelp", "Dragon-Style Twins", "Dragon's Claw", "Dragonlord's Servant", "Dragonscale General", "Drain Life", "Drain Life", "Drain Life", "Drain Power", "Drain Power", "Dreg Mangler", "Drift of the Dead", "Drifting Meadow", "Drove of Elves", "Drudge Skeletons", "Drudge Skeletons", "Drudge Skeletons", "Dryad Militant", "Duergar Hedge-Mage", "Duneblast", "Dungrove Elder", "Duress", "Duress", "Duress", "Durkwood Boars", "Durkwood Boars", "Dusk Imp", "Duskdale Wurm", "Dwarven Demolition Team", "Dwarven Demolition Team", "Dwarven Warriors", "Dwarven Warriors", "Earth Elemental", "Earth Elemental", "Earth Elemental", "Earthbind", "Earthbind", "Earthquake", "Earthquake", "Earwig Squad", "Eater of Hope", "Eidolon of Blossoms", "Electrolyze", "Electrolyze", "Elesh Norn", "Grand Cenobite", "Elite Inquisitor", "Elkin Bottle", "Elven Riders", "Elven Riders", "Elven Riders", "Elves of Deep Shadow", "Elvish Aberration", "Elvish Archers", "Elvish Archers", "Elvish Archers", "Elvish Archers", "Elvish Bard", "Elvish Champion", "Elvish Champion", "Elvish Champion", "Elvish Champion", "Elvish Eulogist", "Elvish Lyrist", "Elvish Mystic", "Elvish Promenade", "Elvish Visionary", "Elvish Visionary", "Elvish Warrior", "Ember Swallower", "Emeria Angel", "Empyrial Armor", "Emrakul", "the Aeons Torn", "Encroaching Wastes", "Energy Flux", "Energy Flux", "Engineered Plague", "Enlightened Tutor", "Enrage", "Entering", "Entomb", "Erhnam Djinn", "Essence Drain", "Essence Flare", "Essence Scatter", "Eternal Dragon", "Eternal Witness", "Evacuation", "Everflowing Chalice", "Evil Presence", "Evil Presence", "Evil Presents", "Evolving Wilds", "Evolving Wilds", "Exalted Angel", "Experiment One", "Eyeblight's Ending", "Fact or Fiction", "Faerie Conclave", "Faithless Looting", "False Orders", "False Orders", "False Prophet", "Fanatic of Xenagos", "Farmstead", "Farmstead", "Farseek", "Fastbond", "Fastbond", "Fated Conflagration", "Fated Intervention", "Fathom Mage", "Fear", "Fear", "Feast of Blood", "Feast of the Unicorn", "Feedback", "Feedback", "Feedback", "Feedback", "Feral Shadow", "Feral Throwback", "Fiery Temper", "Figure of Destiny", "Fire", "Fire Elemental", "Fire Elemental", "Fireball", "Fireball", "Fireball", "Fireball", "Fireball", "Fireball", "Fireball", "Fireball", "Fireblast", "Firebolt", "Firebreathing", "Firebreathing", "Firemane Avenger", "Fireslinger", "Flame Javelin", "Flamerush Rider", "Flametongue Kavu", "Flashfires", "Flashfires", "Flight", "Flight", "Fling", "Fling", "Fling", "Flooded Strand", "Flusterstorm", "Flying Crane Technique", "Foe-Razer Regent", "Fog", "Fog", "Folk of the Pines", "Font of Fertility", "Forbid", "Forbidden Alchemy", "Force of Nature", "Force of Nature", "Force of Nature", "Force of Will", "Force Spike", "Forcefield", "Forcefield", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forest", "Forgestoker Dragon", "Fork", "Fork", "Foul Familiar", "Foundry Champion", "Fraternal Exaltation", "Freewind Falcon", "Frenzied Goblin", "Frost Titan", "Frozen Shade", "Frozen Shade", "Fruitcake Elemental", "Fungal Shambler", "Fungusaur", "Fungusaur", "Furnace of Rath", "Fyndhorn Elves", "Gaea's Blessing", "Gaea's Cradle", "Gaea's Herald", "Gaea's Liege", "Gaea's Liege", "Gangrenous Zombies", "Garruk Wildspeaker", "Garruk", "Apex Predator", "Garruk", "Caller of Beasts", "Garruk's Horde", "Gatekeeper of Malakir", "Gather the Townsfolk", "Gauntlet of Might", "Gauntlet of Might", "Gaze of Granite", "Geist of Saint Traft", "Gemstone Mine", "Genesis", "Genju of the Spires", "Gerrard's Verdict", "Ghor-Clan Rampager", "Ghost-Lit Raider", "Ghostly Prison", "Giant Growth", "Giant Growth", "Giant Growth", "Giant Growth", "Giant Growth", "Giant Growth", "Giant Growth", "Giant Growth", "Giant Spider", "Giant Spider", "Giant Spider", "Giant Spider", "Giant Trap Door Spider", "Giant Trap Door Spider", "Gifts Given", "Gitaxian Probe", "Glacial Ray", "Glasses of Urza", "Glasses of Urza", "Glasses of Urza", "Glasses of Urza", "Gleancrawler", "Glissa", "the Traitor", "Glistener Elf", "Gloom", "Gloom", "Glorious Anthem", "Glorious Anthem", "Glorious Anthem", "Glory", "Go for the Throat", "Goblin Balloon Brigade", "Goblin Balloon Brigade", "Goblin Balloon Brigade", "Goblin Bombardment", "Goblin Digging Team", "Goblin Diplomats", "Goblin Grenade", "Goblin Guide", "Goblin Hero", "Goblin King", "Goblin King", "Goblin King", "Goblin Legionnaire", "Goblin Matron", "Goblin Mime", "Goblin Mutant", "Goblin Mutant", "Goblin Offensive", "Goblin Piker", "Goblin Piledriver", "Goblin Rabblemaster", "Goblin Recruiter", "Goblin Ringleader", "Goblin Sky Raider", "Goblin Snowman", "Goblin Tinkerer", "Goblin Vandal", "Goblin Warchief", "Goblin Warrens", "Goblin Welder", "Golem's Heart", "Gorilla Chieftain", "Gorilla Shaman", "Granite Gargoyle", "Granite Gargoyle", "Granny's Payback", "Grave Titan", "Gravecrawler", "Gravedigger", "Gray Ogre", "Gray Ogre", "Greater Good", "Green Ward", "Green Ward", "Greenweaver Druid", "Grim Haruspex", "Grim Lavamancer", "Griselbrand", "Grisly Salvage", "Grizzly Bears", "Grizzly Bears", "Grizzly Bears", "Grizzly Bears", "Groundbreaker", "Grove of the Guardian", "Gruul Guildmage", "Guardian Angel", "Guardian Angel", "Guerrilla Tactics", "Guerrilla Tactics", "Guul Draz Assassin", "Hada Freeblade", "Hall of Triumph", "Hamletback Goliath", "Hammer of Bogardan", "Hanna", "Ship's Navigator", "Harbinger of the Hunt", "Hardened Scales", "Harmonize", "Harrow", "Healing Salve", "Healing Salve", "Healing Salve", "Healing Salve", "Hedge Troll", "Heir of the Wilds", "Hellspark Elemental", "Helm of Chatzuk", "Helm of Chatzuk", "Helm of Kaldra", "Herald of Anafenza", "Hermit Druid", "Hero of Bladehold", "Hero's Downfall", "Heroes' Bane", "High Sentinels of Arashin", "High Tide", "Hill Giant", "Hill Giant", "Hill Giant", "Hill Giant", "Hill Giant", "Hinder", "Hive Stirrings", "Holy Armor", "Holy Armor", "Holy Strength", "Holy Strength", "Honor of the Pure", "Hordeling Outburst", "Howl from Beyond", "Howl from Beyond", "Howl of the Night Pack", "Howling Mine", "Howling Mine", "Howlpack Alpha", "Hurloon Minotaur", "Hurloon Minotaur", "Hurricane", "Hurricane", "Hurricane", "Hurricane", "Hydra Broodmaster", "Hymn to Tourach", "Hypersonic Dragon", "Hypnotic Specter", "Hypnotic Specter", "Hypnotic Specter", "Hypnotic Specter", "Icatian Javelineers", "Icatian Javelineers", "Ice", "Ice Storm", "Ice Storm", "Iceberg", "Ichiga", "Who Topples Oaks", "Icy Blast", "Icy Manipulator", "Icy Manipulator", "Icy Manipulator", "Icy Manipulator", "Ihsan's Shade", "Illusionary Mask", "Illusionary Mask", "Immaculate Magistrate", "Imperial Recruiter", "Imperious Perfect", "Imperious Perfect", "Impulse", "In Garruk's Wake", "Incinerate", "Incinerate", "Incinerate", "Incinerate", "Incinerate", "Indulgent Tormentor", "Infantry Veteran", "Inferno Titan", "Infest", "Ink-Eyes", "Servant of Oni", "Insidious Bookworms", "Instill Energy", "Instill Energy", "Intuition", "Invisibility", "Invisibility", "Iron Star", "Iron Star", "Ironclaw Orcs", "Ironclaw Orcs", "Ironclaw Orcs", "Ironclaw Orcs", "Ironroot Treefolk", "Ironroot Treefolk", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island", "Island Sanctuary", "Island Sanctuary", "Isochron Scepter", "Ivory Cup", "Ivory Cup", "Ivorytusk Fortress", "Ivorytusk Fortress", "Izzet Charm", "Jace Beleren", "Jace", "Memory Adept", "Jace", "the Living Guildpact", "Jace's Ingenuity", "Jackal Pup", "Jade Monolith", "Jade Monolith", "Jade Statue", "Jade Statue", "Jagged-Scar Archers", "Jalum Tome", "Jaya Ballard", "Task Mage", "Jayemdae Tome", "Jayemdae Tome", "Jayemdae Tome", "Jayemdae Tome", "Jeering Instigator", "Jeskai Ascendancy", "Jokulhaups", "Joraga Warcaller", "Judge's Familiar", "Juggernaut", "Juggernaut", "Juggernaut", "Jump", "Jump", "Kalastria Highborn", "Kamahl", "Pit Fighter", "Kamahl", "Pit Fighter", "Karador", "Ghost Chieftain", "Karakas", "Karametra's Acolyte", "Karma", "Karma", "Karmic Guide", "Karn", "Silver Golem", "Karplusan Forest", "Keldon Warlord", "Keldon Warlord", "Kheru Lich Lord", "Killing Wave", "King Cheetah", "Kiora's Follower", "Kird Ape", "Kitchen Finks", "Kiyomaro", "First to Stand", "Kjeldoran Dead", "Kjeldoran Elite Guard", "Kjeldoran Home Guard", "Kjeldoran Pride", "Knight Exemplar", "Knight of New Alara", "Knight of Stromgald", "Kor Duelist", "Kor Firewalker", "Kor Skyfisher", "Korlash", "Heir to Blackblade", "Kormus Bell", "Kormus Bell", "Kraken's Eye", "Krosan Grip", "Krosan Tusker", "Krosan Warchief", "Kudzu", "Kudzu", "Lady Orca", "Lance", "Lance", "Land Tax", "Laquatus's Champion", "Lat-Nam's Legacy", "Latch Seeker", "Lava Axe", "Lava Burst", "Leatherback Baloth", "Legions of Lim-Dûl", "Ley Druid", "Ley Druid", "Lhurgoyf", "Library of Leng", "Library of Leng", "Lich", "Lich", "Life", "Lifeforce", "Lifeforce", "Lifelace", "Lifelace", "Lifetap", "Lifetap", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Bolt", "Lightning Dragon", "Lightning Elemental", "Lightning Greaves", "Lightning Helix", "Lightning Hounds", "Lightning Rift", "Liliana of the Dark Realms", "Liliana Vess", "Liliana Vess", "Liliana's Specter", "Lim-Dûl's High Guard", "Lim-Dûl's High Guard", "Lingering Souls", "Living Artifact", "Living Artifact", "Living Death", "Living Lands", "Living Lands", "Living Wall", "Living Wall", "Living Wish", "Llanowar Elves", "Llanowar Elves", "Llanowar Elves", "Llanowar Elves", "Llanowar Elves", "Lobotomy", "Longbow Archer", "Lord of Atlantis", "Lord of Atlantis", "Lord of Atlantis", "Lord of Shatterskull Pass", "Lord of the Pit", "Lord of the Pit", "Lost Soul", "Lost Soul", "Lotus Bloom", "Lotus Cobra", "Loxodon Warhammer", "Lu Bu", "Master-at-Arms", "Lu Bu", "Master-at-Arms", "Ludevic's Abomination", "Ludevic's Test Subject", "Lure", "Lure", "Lys Alana Huntmaster", "Mad Auntie", "Maelstrom Pulse", "Magical Hack", "Magical Hack", "Magister of Worth", "Magma Jet", "Magma Spray", "Magmaquake", "Mahamoti Djinn", "Mahamoti Djinn", "Mahamoti Djinn", "Malfegor", "Man-o'-War", "Mana Crypt", "Mana Flare", "Mana Flare", "Mana Leak", "Mana Leak", "Mana Short", "Mana Short", "Mana Tithe", "Mana Vault", "Mana Vault", "Manabarbs", "Manabarbs", "Mardu Ascendancy", "Mardu Shadowspear", "Marisi's Twinclaws", "Master of Pearls", "Master's Call", "Maul Splicer", "Mayor of Avabruck", "Maze of Ith", "Maze's End", "Meddling Mage", "Meekstone", "Meekstone", "Megantic Sliver", "Megrim", "Melek", "Izzet Paragon", "Memnite", "Memoricide", "Memory Lapse", "Mercurial Pretender", "Merfolk Mesmerist", "Merfolk of the Pearl Trident", "Merfolk of the Pearl Trident", "Merfolk of the Pearl Trident", "Merfolk of the Pearl Trident", "Merrow Reejerey", "Mesa Pegasus", "Mesa Pegasus", "Mesa Pegasus", "Mesa Pegasus", "Mind Control", "Mind Control", "Mind Rot", "Mind Shatter", "Mind Spring", "Mind Stone", "Mind Twist", "Mind Twist", "Mind Warp", "Mind's Desire", "Mirari's Wake", "Mirran Crusader", "Mirri", "Cat Warrior", "Mise", "Mishra's Factory", "Mishra's Toy Workshop", "Mistfolk", "Mitotic Slime", "Mogg Fanatic", "Mogg Fanatic", "Mogg Fanatic", "Mogg Flunkies", "Mogg Raider", "Molimo", "Maro-Sorcerer", "Mondronen Shaman", "Mons's Goblin Raiders", "Mons's Goblin Raiders", "Mons's Goblin Raiders", "Mons's Goblin Raiders", "Monstrous Hound", "Moonglove Winnower", "Moonsilver Spear", "Morphling", "Mortify", "Mortivore", "Mother of Runes", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mountain", "Mox Emerald", "Mox Emerald", "Mox Jet", "Mox Jet", "Mox Pearl", "Mox Pearl", "Mox Ruby", "Mox Ruby", "Mox Sapphire", "Mox Sapphire", "Mulldrifter", "Murderous Redcap", "Murk Dwellers", "Murk Dwellers", "Muscle Sliver", "Mutavault", "Mwonvuli Beast Tracker", "Mycoid Shepherd", "Myr Enforcer", "Myr Superion", "Nameless Inversion", "Narset", "Enlightened Master", "Natural Order", "Natural Selection", "Natural Selection", "Natural Spring", "Naturalize", "Nature's Spiral", "Naughty", "Naya Sojourners", "Nearheath Stalker", "Necromaster Dragon", "Necropolis Fiend", "Necropolis Fiend", "Necropotence", "Necrosavant", "Negate", "Negate", "Nekusar", "the Mindrazer", "Nessian Wilds Ravager", "Nether Shadow", "Nether Shadow", "Nettling Imp", "Nettling Imp", "Nevinyrral's Disk", "Nevinyrral's Disk", "Nevinyrral's Disk", "Nice", "Nighthowler", "Nightmare", "Nightmare", "Nightveil Specter", "Nissa Revane", "Nissa", "Worldwaker", "Nissa's Chosen", "Niv-Mizzet", "the Firemind", "Noble Hierarch", "Northern Paladin", "Northern Paladin", "Oath of Druids", "Obelisk of Alara", "Oblivion Ring", "Obsianus Golem", "Obsianus Golem", "Ogre Arsonist", "Ogre Battledriver", "Ojutai's Command", "Okina Nightwatch", "Oloro", "Ageless Ascetic", "Oona's Blackguard", "Ophidian", "Orcish Artillery", "Orcish Artillery", "Orcish Artillery", "Orcish Artillery", "Orcish Cannoneers", "Orcish Healer", "Orcish Lumberjack", "Orcish Oriflamme", "Orcish Oriflamme", "Orcish Oriflamme", "Orcish Oriflamme", "Order of the White Shield", "Orim's Chant", "Oros", "the Avenger", "Overbeing of Myth", "Overrun", "Overrun", "Overtaker", "Overwhelming Forces", "Ovinomancer", "Oxidize", "Pacifism", "Pain Seer", "Paralyze", "Paralyze", "Path to Exile", "Pathrazer of Ulamog", "Peace Talks", "Pearled Unicorn", "Pearled Unicorn", "Pearled Unicorn", "Pearled Unicorn", "Pegasus Charger", "Pegasus Stampede", "Pendelhaven", "Pendelhaven", "Pernicious Deed", "Personal Incarnation", "Personal Incarnation", "Pestilence", "Pestilence", "Phalanx Leader", "Phantasmal Fiend", "Phantasmal Fiend", "Phantasmal Fiend", "Phantasmal Forces", "Phantasmal Forces", "Phantasmal Terrain", "Phantasmal Terrain", "Phantom Monster", "Phantom Monster", "Phantom Monster", "Phantom Warrior", "Phyrexian Dreadnought", "Phyrexian Metamorph", "Phyrexian Negator", "Phyrexian Rager", "Phyrexian War Beast", "Phyrexian War Beast", "Phytotitan", "Pillage", "Pillage", "Pillar of Flame", "Pirate Ship", "Pirate Ship", "Plague Myr", "Plague Rats", "Plague Rats", "Plague Stinger", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plains", "Plateau", "Plateau", "Polluted Delta", "Polluted Mire", "Ponder", "Portent", "Pouncing Jaguar", "Powder Keg", "Power Leak", "Power Leak", "Power Sink", "Power Sink", "Power Sink", "Power Sink", "Power Surge", "Power Surge", "Priest of Titania", "Priest of Urabrask", "Primeval Titan", "Primordial Hydra", "Pristine Skywise", "Pristine Talisman", "Prodigal Pyromancer", "Prodigal Sorcerer", "Prodigal Sorcerer", "Prodigal Sorcerer", "Prognostic Sphinx", "Prophet of Kruphix", "Proposal", "Psionic Blast", "Psionic Blast", "Psionic Blast", "Psychatog", "Psychic Venom", "Psychic Venom", "Purelace", "Purelace", "Putrefy", "Pyroclasm", "Pyroclasm", "Pyrokinesis", "Pyrotechnics", "Pyrotechnics", "Pyrotechnics", "Qasali Pridemage", "Questing Phelddagrif", "Quirion Ranger", "Rage Reflection", "Raging Goblin", "Raging Kavu", "Raging River", "Raging River", "Raise Dead", "Raise Dead", "Raise Dead", "Raise Dead", "Rakdos Cackler", "Rakdos Guildmage", "Rakshasa Vizier", "Rakshasa Vizier", "Rampaging Baloths", "Rampant Growth", "Rampant Growth", "Rancor", "Ranger en-Vec", "Ratchet Bomb", "Rathi Assassin", "Rattleclaw Mystic", "Rattleclaw Mystic", "Ravenous Baloth", "Ravenous Demon", "Ravenous Rats", "Reanimate", "Reaper of the Wilds", "Reciprocate", "Reckless Wurm", "Reclamation Sage", "Recollect", "Red Elemental Blast", "Red Elemental Blast", "Red Ward", "Red Ward", "Regeneration", "Regeneration", "Regrowth", "Regrowth", "Regrowth", "Reinforcements", "Reliquary Tower", "Remand", "Remove Soul", "Render Silent", "Resolute Archangel", "Restoration Angel", "Resurrection", "Resurrection", "Resurrection", "Retaliator Griffin", "Revenant", "Reverse Damage", "Reverse Damage", "Reverse Damage", "Rewind", "Reya Dawnbringer", "Rhox", "Rhox War Monk", "Rift Bolt", "Righteousness", "Righteousness", "Riku of Two Reflections", "Rise from the Grave", "River Boa", "River Boa", "Roar of the Wurm", "Robot Chicken", "Roc of Kher Ridges", "Roc of Kher Ridges", "Rock Hydra", "Rock Hydra", "Rod of Ruin", "Rod of Ruin", "Rod of Ruin", "Rod of Ruin", "Roughshod Mentor", "Royal Assassin", "Royal Assassin", "Royal Assassin", "Royal Assassin", "Rubblehulk", "Rukh Egg", "Runeclaw Bear", "Ryusei", "the Falling Star", "Sacred Mesa", "Sacrifice", "Sacrifice", "Sage of the Inward Eye", "Sage of the Inward Eye", "Sage-Eye Avengers", "Sakura-Tribe Elder", "Sakura-Tribe Elder", "Sakura-Tribe Elder", "Sakura-Tribe Elder", "Samite Healer", "Samite Healer", "Samite Healer", "Sandsteppe Mastodon", "Savage Lands", "Savannah", "Savannah", "Savannah Lions", "Savannah Lions", "Scaleguard Sentinels", "Scars of the Veteran", "Scathe Zombies", "Scathe Zombies", "Scathe Zombies", "Scavenger Folk", "Scavenging Ghoul", "Scavenging Ghoul", "Scavenging Ooze", "Scent of Cinder", "Scourge of Fleets", "Scragnoth", "Scrubland", "Scrubland", "Scryb Sprites", "Scryb Sprites", "Scryb Sprites", "Scryb Sprites", "Sea Serpent", "Sea Serpent", "Seal of Cleansing", "Searing Blaze", "Searing Spear", "Season's Beatings", "Sedge Troll", "Sedge Troll", "Selkie Hedge-Mage", "Sengir Vampire", "Sengir Vampire", "Sengir Vampire", "Serra Angel", "Serra Angel", "Serra Angel", "Serra Angel", "Serra Avatar", "Serra Avatar", "Serra Avenger", "Serrated Arrows", "Serrated Arrows", "Serum Visions", "Severed Legion", "Shamanic Revelation", "Shanodin Dryads", "Shanodin Dryads", "Shard Phoenix", "Shard Phoenix", "Shard Phoenix", "Shatter", "Shatter", "Shatter", "Sheoldred", "Whispering One", "Shichifukujin Dragon", "Shield of Kaldra", "Shipbreaker Kraken", "Shivan Dragon", "Shivan Dragon", "Shivan Dragon", "Shivan Dragon", "Shock", "Shock", "Show and Tell", "Shrapnel Blast", "Shriekmaw", "Shrine of Burning Rage", "Sidisi", "Brood Tyrant", "Siege Dragon", "Siege Rhino", "Sign in Blood", "Signal Pest", "Silent Sentinel", "Silent Specter", "Silver Drake", "Silver Knight", "Silverblade Paladin", "Simulacrum", "Simulacrum", "Sin Collector", "Sinkhole", "Sinkhole", "Sinkhole", "Siren's Call", "Siren's Call", "Skarrg Goliath", "Skinrender", "Skirk Marauder", "Skittering Skirge", "Skull Catapult", "Skyknight Legionnaire", "Slave of Bolas", "Sleight of Mind", "Sleight of Mind", "Slice and Dice", "Slippery Karst", "Slith Firewalker", "Slith Firewalker", "Smoke", "Smoke", "Smoldering Crater", "Smother", "Snapping Drake", "Snapping Drake", "Sneak Attack", "Snow Devil", "Snow Mercy", "Sol Ring", "Sol Ring", "Sol Ring", "Soltari Priest", "Soltari Priest", "Soltari Priest", "Soltari Priest", "Sorceress Queen", "Sorceress Queen", "Soul Burn", "Soul Burn", "Soul Collector", "Soul Net", "Soul Net", "Soul of Ravnica", "Soul of Zendikar", "Sparksmith", "Spawn of Thraxes", "Spectral Bears", "Spell Blast", "Spell Blast", "Spellstutter Sprite", "Spike Feeder", "Spined Wurm", "Spined Wurm", "Spiritmonger", "Splendid Genesis", "Sprouting Thrinax", "Squadron Hawk", "Squelching Leeches", "Staff of Nin", "Staggershock", "Standstill", "Stasis", "Stasis", "Staunch Defenders", "Steal Artifact", "Steal Artifact", "Stealer of Secrets", "Steel Hellkite", "Steward of Valeron", "Stifle", "Stocking Tiger", "Stoke the Flames", "Stone Giant", "Stone Giant", "Stone Rain", "Stone Rain", "Stone Rain", "Stone-Tongue Basilisk", "Storm Crow", "Storm Elemental", "Storm Entity", "Storm Shaman", "Storm Shaman", "Stormblood Berserker", "Strangleroot Geist", "Stream of Life", "Stream of Life", "Strip Mine", "Stroke of Genius", "Stupor", "Sudden Shock", "Sulfurous Springs", "Sultai Ascendancy", "Sultai Ascendancy", "Sultai Charm", "Sun Titan", "Sunblast Angel", "Sunglasses of Urza", "Sunglasses of Urza", "Supplant Form", "Supreme Verdict", "Surgical Extraction", "Surging Flame", "Surrak Dragonclaw", "Survival of the Fittest", "Suspension Field", "Suture Priest", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Swamp", "Sword of Feast and Famine", "Sword of Fire and Ice", "Sword of Kaldra", "Sword of Light and Shadow", "Swords to Plowshares", "Swords to Plowshares", "Swords to Plowshares", "Swords to Plowshares", "Swords to Plowshares", "Swords to Plowshares", "Sylvan Caryatid", "Sylvan Ranger", "Sylvan Ranger", "Syphon Mind", "Taiga", "Taiga", "Talara's Battalion", "Tectonic Edge", "Teetering Peaks", "Tempered Steel", "Temple of Mystery", "Temur Ascendancy", "Temur War Shaman", "Tendrils of Agony", "Terastodon", "Terminate", "Terminate", "Terror", "Terror", "Terror", "Terror", "Terror", "Terror", "Terror", "Terror", "Thawing Glaciers", "The Hive", "The Hive", "The Rack", "Thicket Basilisk", "Thicket Basilisk", "Thieving Magpie", "Thirst for Knowledge", "Thopter Assembly", "Thoughtlace", "Thoughtlace", "Thousand Winds", "Thran Quarry", "Throne of Bone", "Throne of Bone", "Thunderbreak Regent", "Tidehollow Sculler", "Tidings", "Timber Wolves", "Timber Wolves", "Time Vault", "Time Vault", "Time Walk", "Time Walk", "Time Warp", "Timetwister", "Timetwister", "Tinder Wall", "Tormented Hero", "Tormented Soul", "Tormod's Crypt", "Tovolar's Magehunter", "Tradewind Rider", "Trail of Mystery", "Trained Armodon", "Tranquility", "Tranquility", "Trap Essence", "Treasure Hunt", "Treasure Hunt", "Treasure Mage", "Treasury Thrull", "Treetop Village", "Treetop Village", "Treva", "the Renewer", "Troll Ascetic", "Tromokratis", "Tropical Island", "Tropical Island", "Trostani's Summoner", "Tsunami", "Tsunami", "Tundra", "Tundra", "Tunnel", "Tunnel", "Turnabout", "Twiddle", "Twiddle", "Twiddle", "Twiddle", "Two-Headed Dragon", "Two-Headed Giant of Foriys", "Two-Headed Giant of Foriys", "Uktabi Orangutan", "Umezawa's Jitte", "Underground River", "Underground Sea", "Underground Sea", "Underworld Dreams", "Underworld Dreams", "Unholy Strength", "Unholy Strength", "Unholy Strength", "Unholy Strength", "Unmake", "Unsummon", "Unsummon", "Unsummon", "Unsummon", "Unsummon", "Untamed Wilds", "Untamed Wilds", "Urborg Mindsucker", "Urza's Factory", "Uthden Troll", "Uthden Troll", "Uthden Troll", "Utter End", "Utter End", "Valakut", "the Molten Pinnacle", "Vampire Bats", "Vampire Bats", "Vampire Nighthawk", "Vampire Nocturnus", "Vampire Nocturnus", "Vampiric Tutor", "Vampirism", "Vault Skirge", "Vendilion Clique", "Vengevine", "Verdant Force", "Verduran Enchantress", "Verduran Enchantress", "Vesuvan Doppelganger", "Vesuvan Doppelganger", "Veteran Bodyguard", "Veteran Bodyguard", "Vexing Shusher", "Viashino Sandstalker", "Vigor", "Villainous Wealth", "Vindicate", "Vindicate", "Viscerid Drone", "Voidmage Husher", "Voidmage Prodigy", "Voidslime", "Volcanic Dragon", "Volcanic Eruption", "Volcanic Eruption", "Volcanic Fallout", "Volcanic Geyser", "Volcanic Hammer", "Volcanic Island", "Volcanic Island", "Walking Wall", "Wall of Air", "Wall of Air", "Wall of Blossoms", "Wall of Bone", "Wall of Bone", "Wall of Bone", "Wall of Bone", "Wall of Brambles", "Wall of Brambles", "Wall of Fire", "Wall of Fire", "Wall of Ice", "Wall of Ice", "Wall of Omens", "Wall of Roots", "Wall of Spears", "Wall of Stone", "Wall of Stone", "Wall of Swords", "Wall of Swords", "Wall of Water", "Wall of Water", "Wall of Wood", "Wall of Wood", "Wall of Wood", "Wanderlust", "Wanderlust", "War Mammoth", "War Mammoth", "War Mammoth", "War Mammoth", "Warleader's Helix", "Warmonger", "Warp Artifact", "Warp Artifact", "Warp Artifact", "Warp Artifact", "Warrior's Honor", "Wash Out", "Wasteland", "Wasteland", "Watchwolf", "Water Elemental", "Water Elemental", "Weakness", "Weakness", "Weakness", "Weakness", "Web", "Web", "Wee Dragonauts", "Whalebone Glider", "Wheel of Fortune", "Wheel of Fortune", "Wheel of Fortune", "Whip of Erebos", "Whipcorder", "Whirling Dervish", "Whirling Dervish", "Whirling Dervish", "Whirling Dervish", "Whirling Dervish", "White Knight", "White Knight", "White Knight", "White Knight", "White Ward", "White Ward", "Wicked Reward", "Wild Growth", "Wild Growth", "Wild Mongrel", "Wild Nacatl", "Will-o'-the-Wisp", "Will-o'-the-Wisp", "Willbender", "Wilt-Leaf Cavaliers", "Windswept Heath", "Wing Shards", "Wings of Aesthir", "Winter Blast", "Winter Blast", "Winter Orb", "Winter Orb", "Withered Wretch", "Wonder", "Wood Elves", "Wooded Foothills", "Wooden Sphere", "Wooden Sphere", "Woolly Mammoths", "Woolly Spider", "Woolly Spider", "Woolly Thoctar", "Word of Command", "Word of Command", "Wrath of God", "Wrath of God", "Wrath of God", "Wren's Run Packmaster", "Wren's Run Vanquisher", "Wurm's Tooth", "Wurmcoil Engine", "Xathrid Gorgon", "Xathrid Necromancer", "Xiahou Dun", "the One-Eyed", "Yavimaya Ancients", "Yavimaya Ancients", "Yavimaya Ants", "Yawgmoth's Will", "Yixlid Jailer", "Youthful Knight", "Yule Ooze", "Zameck Guildmage", "Zephyr Falcon", "Zephyr Falcon", "Zoetic Cavern", "Zombie Apocalypse", "Zombie Master", "Zombie Master", "Zombify", "Zuran Spellcaster", "Zurgo Helmsmasher");
    completeCardsWithoutMultiverseIds(cardNamesWithoutMultiverseId);
  }

  private void completeCardsWithoutMultiverseIds(final Set<String> cardNamesWithoutMultiverseId) throws IOException, SQLException {
    final List<String> stillWithoutId = Lists.newArrayList();
    final List<Card> cardBlock = Lists.newArrayList();

    for (final String cardName : cardNamesWithoutMultiverseId) {
      final String cardRecoveryUrl = URIUtil.encodeQuery(cardByNameUri + cardName);
      final String namesWithIdsJson = CardCache.fetch(cardRecoveryUrl);
      final JSONArray namesWithIds = new JSONArray(namesWithIdsJson);
      for (final Object nameWithIdObj : namesWithIds) {
        final JSONObject nameWithId = (JSONObject) nameWithIdObj;
        final String name = nameWithId.getString("name");
        if (name == null || !name.equals(cardName)) {
          continue;
        }
        if (nameWithId.isNull("id") || nameWithId.getInt("id") == 0) {
          stillWithoutId.add(cardName);
          continue;
        }
        cardBlock.add(new Card(cardName, nameWithId.getInt("id")));

        if (cardBlock.size() >= blockSize) {
          writeBlockToDB(cardBlock, conn);
        }
//        final String cardByIdFullUrl = cardByIdUri + multiverseId;
//        final String cardJson = CardCache.fetch(cardByIdFullUrl);
        //todo: a little code duplication here
//        final JSONObject card = new JSONObject(cardJson);
//        final String cardName = card.getString("name");
//        final int multiverseId = card.getInt("multiverseid");
      }
    }
    writeBlockToDB(cardBlock, conn);
    System.out.println("stillWithoutId: " + stillWithoutId);
  }


  @Test
  /**
   * Store only one instance for each card came, the one with max multiverseID
   */
  public void test() throws Exception {
    final Set<String> cardNamesWithoutMultiverseId = Sets.newHashSet();
    final Map<String, Card> multiverseIdToCard = Maps.newHashMap();

    readCards(cardNamesWithoutMultiverseId, multiverseIdToCard);
    handleCardsWithoutMultiverseId(cardNamesWithoutMultiverseId, multiverseIdToCard);
    writeCardsToDB(multiverseIdToCard);
  }

  private void writeCardsToDB(final Map<String, Card> multiverseIdToCard) throws SQLException, IllegalAccessException, InstantiationException, ClassNotFoundException {

    try (final Connection conn = DBConnectionManager.getConnection()) {
      System.out.println("Started writing cards at" + new DateTime());
      final List<Card> cards = Lists.newArrayList(multiverseIdToCard.values());
      cards.sort((o1, o2) -> o1.multiverseId - o2.multiverseId);

      final ArrayList<Card> cardBlock = new ArrayList<>(blockSize);

      for (final Card card : cards) {
        cardBlock.add(card);
        if (cardBlock.size() == blockSize) {
          writeBlockToDB(cardBlock, conn);
        }
      }
      if (cardBlock.size() > 0) {
        writeBlockToDB(cardBlock, conn);
      }
      System.out.println("Finished writing cards at" + new DateTime());
    }
  }

  private void handleCardsWithoutMultiverseId(final Set<String> cardNamesWithoutMultiverseId, final Map<String, Card> cardToMultiverseId) {
    int unknownMultiverseIds = cardNamesWithoutMultiverseId.size();

    for (final Iterator<String> iter = cardNamesWithoutMultiverseId.iterator(); iter.hasNext() ;) {
      final Card card = cardToMultiverseId.get(iter.next());
      if (card != null && card.multiverseId != null) {
        unknownMultiverseIds--;
        iter.remove();
      }
    }
    System.out.println("Real number of cards with unknown multiverse id: " + unknownMultiverseIds);
    System.out.println(cardNamesWithoutMultiverseId);
  }

  private void readCards(final Set<String> cardNamesWithoutMultiverseId, final Map<String, Card> cardToMultiverseId) throws IOException {
    System.out.println("Started reading cards at " + new DateTime());
    int lastPage, currPage;
    currPage = 1;
    do {
      try {
        if (currPage % 10 == 0) {
          System.out.println("At page " + currPage + " time: " + new DateTime());
        }
        final String uri = allCardsUri + currPage;
        final String json = CardCache.fetch(uri);
        currPage++;
        final JSONObject jsonObject = new JSONObject(json);
        lastPage = Integer.valueOf(jsonObject.getJSONObject("links").getString("last").split("=")[1]);
        final JSONArray cards = jsonObject.getJSONArray("cards");

        for (final Object cardObj : cards) {
          final JSONObject card = (JSONObject) cardObj;
          final String cardName = card.getString("name");
          if (card.isNull("multiverseid") || card.getInt("multiverseid") == 0) {
            cardNamesWithoutMultiverseId.add(cardName);
          } else {
            final int multiverseId = card.getInt("multiverseid");
            Card storedCard = cardToMultiverseId.get(cardName);
            storedCard = storedCard == null || multiverseId > storedCard.multiverseId ? new Card(cardName, multiverseId) : storedCard;
            //Store only one instance for each card came, the one with max multiverseID
            cardToMultiverseId.put(cardName, storedCard);
          }
        }

      } catch (final Exception e) {
        System.out.println("Exception on page " + currPage + ": " + e.toString());
        throw e;
      }
    } while (currPage <= lastPage);

    System.out.println("Last page is: " + lastPage);
    System.out.println("Finished reading cards at" + new DateTime());
  }


  private void writeBlockToDB(final List<Card> cardBlock, final Connection conn) throws SQLException {
    System.out.println("Started writing block at: " + LocalDateTime.now());
    if (cardBlock.isEmpty()) {
      return;
    }
    final StringBuilder sb = new StringBuilder("INSERT INTO cd_card (cd_multiverse_id, cd_name) VALUES ");

    for(final Card card : cardBlock) {
      final String name = card.name.replace("'", ""); //TODO: removing the ' char, keep that in mind when comparing card names
      final Integer multiverseId = card.multiverseId;
      sb.append("(").append(multiverseId).append(",'").append(name).append("'),");
    }
    cardBlock.clear();
    sb.deleteCharAt(sb.length() - 1); // Deleting last comma
    final String query = sb.toString();

    try (final Statement stmt = conn.createStatement()) {
      stmt.execute(query);
    } catch (final Exception e) {
      System.out.println("Failed to insert. Error: " + e.getMessage() + " query: " + query);
      e.printStackTrace();
      throw e;
    } finally {
      System.out.println("Finished writing block at: " + LocalDateTime.now());
    }
  }

  @Test
  public void testCacheLoad() throws Exception {
    final Map<String, Integer> cardNameToId = CardCache.get();

    final String chronozoa = "Chronozoa";
    final int chronozoaId = getCardId(chronozoa);

    Assert.assertNotNull(cardNameToId);

    final int size = cardNameToId.size();

    System.out.println(size);
    Assert.assertTrue(size > 1000);
    Assert.assertTrue(size < 100 * 1000);

    final int cachedChronozoaId = cardNameToId.get(chronozoa);
    Assert.assertEquals(chronozoaId, cachedChronozoaId);
  }

  private int getCardId(final String cardName) throws Exception {
    Class.forName("com.mysql.jdbc.Driver").newInstance();
    final Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/mtg?user=root&password=Qwer812$");

    final Statement statement = conn.createStatement();
    final String query = String.format("SELECT cd_id FROM cd_card WHERE cd_name = '%s' ", cardName);
    final ResultSet resultSet = statement.executeQuery(query);

    if (resultSet.next()) {
      return resultSet.getInt(1);
    } else {
      return NO_CARD_ID;
    }
  }
}