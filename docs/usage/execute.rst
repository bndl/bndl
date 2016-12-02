Execute
=======

Execute tasks on workers
Tasks run as part of a Job
Jobs may be composed of multiple Stages
BSP model


.. uml:: 
   :width: 1cm
   
   @startuml
   [*] --> ready
   [*] --> blocked
   blocked --> ready
   ready --> pending
   pending --> done
   done --> [*]
   @enduml
   
   