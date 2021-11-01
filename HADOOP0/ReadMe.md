**************** Mode d'emploi Hidoop ***********************

 *** Le dossier src/config/hdfsclientconfig contient:
   * listWorkerURL.listeurl : contient les urls des workers (port, machine, id)
   * namenode.url : contient l'url du namenode
   * portNoeud.url : contient les ports des HdfsServeurs
   * serverHDFS.listeurl : contient les machines sur lesquelles on lance les 
HdfsServeurs
   * confStructure.conf : contient le nom du fichier sur lequel on travaille

 *** src/lanceur.sh et src/arret.sh :
Les changements effectués sur les fichiers de ce dossier doivent obligatoirement
être faits sur les fichiers src/lanceur.sh et src/arret.sh. Il faudra également
changer le directory DIR contenant l'emplacement du dossier src. NBServer est
le nombre de serveurs lancés.

 *** Lancement de l'application :
Dans src:

  1- javac */*.java
  2- ./lanceur.sh : lancer l'application
  3- ./arret.sh : arrêter tous les daemons

Les fragements du fichiers volumineux seront stockés dans le dossier nosave de
la session et le résultat du traitement sera dans le même emplacement que le 
fichier à traiter.



