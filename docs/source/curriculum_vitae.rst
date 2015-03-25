
Curriculum vitae
================

Alain Spineux

alain.spineux@gmail.com



Recent Projects
===============

These are relevant projects I have worked on recently. 

2010 Web232
-----------

Web232 is a small appliance controlling a professional 
coffee machine to record its use by the waiters.
Waiters unlock the machine with a personal key and every
drinkable are logged on the waiter's account.
At the end of the day the CMS downloads data and
reports any differences with the waiter's invoice.

Web232 run on a small Soekris box running **Linux** **OpenWrt**.
A **Python** script read the two **RS232** serial ports and lock/unlock
the coffee machine via an inboard **IO** port. 
Configuration and status can be controlled by a self-made web interface 
running Python's **Pylons** frameworks. The CMS dialog through the web interface
and exchange **XML** data. 
The appliance can be remotely controlled from anywhere at user request
with the help of self-made :ref:`TCP Proxy Reflector <tcpr>` framework.

2011 Mail2XML
-------------

Mail to XML is an application that read mails from a mailbox, 
save contents and attachments into files and generate an
**XML** file including the META informations about the email
and references to the attached files.
Most CMS applications can import such XML file 
to integrate data coming from emails.

Mail2xml is written in **Python** and sparked off 
the self-made :ref:`pyzmail <pyzmail>` library.

2011 Virtualization of a SCO Unix
---------------------------------

This is the funny things that people ask me to do.
The hard disk was failing and I had to migrate the host into a 
**VMware** Virtual machine without any tools or official procedure. 
I have migrated the box without leaving home or even customer 
intervention on the server and without interrupting the server
itself. I had to recompile the kernel to change the disk controller and
the network adapter. The migration was a success.

Personal Projects
=================

I have developed these projects on my own, motivated by my needs in 
my day to day work. I did 99% of the work alone.


2007 Emailgency
---------------

From http://www.emailgency.com

    Emailgency is an outsourced mail service running in parallel 
    with your mail server and catching your emails when it is unavailable.
    
    Until your server is back on-line, you can connect to our server to read 
    and send your emails. When your server is back, you can flush pending emails 
    to receive your mails in your usual mail environment.

A nice slide show to understand how it works: http://www.emailgency.com/howitworks_anim.en.html

Except for the look and feel of the site, the nice logo and the draw of the 
animation above, I made everything myself.

All the mail components come from the `Kolab <http://www.kolab.org>`__ of
which I used all the installation procedure. I had to rewrite all the glue myself
to adapt to the *Emailgency's* requirements using **bash** and **Python** scripts.

The web interface (`screenshots <http://www.emailgency.com/support/getstarted/index.en.html>`__)
use the Python's `Turbogears <http://www.turbogears.org>`__ framework.

A big part of the works was at a lower level, this include a lot of
self made Python code to handle **SMTP**, **IMAP** and **LDAP** protocols. 
I have written and published a lot of patches for a lot of Python's Libraries, 
even the for the well-respected `cyrus imap <http://www.cyrusimap.org>`__ contains 
now some of my code and ideas.
  
Even if the merchandising was not very active, the current server host about 40 domains
since 4 years without no more than 1 or 2 hours of down time (to replace the 
dead motherboard).

Every tasks, from the installation to the day to day management have been 
scripted and shortly documented. I can install an *Emailgency* server from 
scratch in minutes. Backup are automated. A *test suite* can deeply test any
features before any update. Most important features are monitored in real-time, 
and a second level monitoring running at home can even send SMS. 

2008 MKSBackup
--------------

From http://www.magikmon.com/mksbackup/index.en.html

    MKSBackup is a free front-end for common backup tools like MS Windows ntbackup, 
    and it successor wbadmin, Un*x tools like tar, but also popular ghettoVCB to 
    backup Virtual Machine on **VMware** ESX(i) host.
    MKSBackup is developed in **Python** and is available for Microsoft **Windows**, **Linux** and other Un*x systems.
    MKSBackup is driven by a *command line* interface and jobs are defined in an INI file. 
    Its main feature is to send an **email** report including log files and hints that 
    could give the user confidence about the status of its backup. 

MKSBackup is a great success, a lot of people are using it today. It is the source of
the greatest part of mail reports received by :ref:`MagiKmon <magikmon>`.

Read the user's `feedback <http://forum.magiksys.net/viewforum.php?f=1>`__.

.. _magikmon:

2009 MagiKmon
-------------

From http://www.magikmon.com

    The MagiKmon provide an original outsourced monitoring solution to small offices 
    and to value added IT companies having in charge multiple SoHo.

MagiKmon provides smart and innovative monitoring features without requiring
any additional software. 
  
`Screenshot <http://www.magikmon.com/support/getstarted/index.en.html>`__ and
User's `feedback <http://forum.magiksys.net/viewforum.php?f=10>`__. 

All the components are written in **Python**: The *agents* monitorings 
the **services** to the web interface running on that use `Turbogears 2 <http://www.turbogears.org>`__.

The most original feature is the **backup monitoring** that receive and parse 
email reports from multiple backup tools and generate a nice and colored
`report <http://www.magikmon.com/support/getstarted/backupmon/backupmon.en.html#time_line>`__,
and raise mail or web alerts when the backup fail for too long.

Another original feature are the event and mail *guards* that expect to receive
emails or HTTP requests at regular interval. These can be used by user's script
to easily monitor about anything.  
  


.. _tcpr:

2010 TCP Proxy Reflector 
------------------------

http://blog.magiksys.net/software/tcp-proxy-reflector

TCPR is the kind of framework the well known `LogMeIn <http://www.logmein.com/>`__
could be based on. It include a server, client and console to manage any
kind of protocol.  With TCPR you can install your own server, manage your own users
and allows any kind of protocol. You can take control of your router through *HTTP*
or your PC at home using *VNC* or take control of your father's laptop running Linux
via a SSH connection. 
  
The library and programs are written in **Python** using the asynchronous 
**asyncore** library that allow to manager a high numbers of connections
without over-loading the server.

I use it to support the *applianceq* I have sold, and manage my father's laptop.
 
 
.. _pyzmail:

2011 pyzmail
------------

From http://www.magiksys.net/pyzmail/

    pyzmail is a high level mail library for **Python**. It provides functions and 
    classes that help to read, compose and send emails. *pyzmail* exists because 
    their is no reasons that handling mails with Python would be more difficult 
    than with popular mail clients like Outlook or Thunderbird. *pyzmail* hide the 
    difficulties of the MIME structure and MIME encoding/decoding. It also hide 
    the problem of the internationalized header encoding/decoding.
    
pyzmail is used by other of my projects and has been tested over 20.000 emails from
diversified sources and languages (including a lot of Chinese emails).
  

2011 ddumbfs
------------

from http://www.magiksys.net/ddumbfs/

    ddumbfs is a filesystem for **Linux** doing de-duplication. 
    It use the **FUSE** environment and is released under the terms of the GPL. 
    De-duplication is a technique to avoid data duplication on disks and to 
    increase its virtual capacity.

*ddumbfs* is written in **C** to maximize the performances and is probably 
the fastest available solution for Linux now.
**ddumbfs** should be officially released this week.


Employments
============

1995-2000 C & C++ developer
---------------------------
I worked for **Université catholique de Louvain-la-neuve** on two R&D projects
related to mechanics:

- One about simulation of mechanical system. I was in charge of the rendering of the simulation in 3D using OpenGL in C and Linux and Un*x.
- The 2nd about the optimization of assembly lines. Mostly in C++ under Windows, using *ObjecStore*, an object oriented database.  

2000-2006 System Engineer
-------------------------
KEYSOURCE S.C.R.L. at Brussels Belgium was involved in IT Outsoursing 
for Small business (up to 100 users) and Multinational Corporation.
I came with my Linux Background and we started to offer Linux solutions. 
I developed some Linux appliances: an "E-Key", a mail virus filter 
and "B-Key", a centralized backup solution.
Because of my skill, I was mostly in charge of everything related
to the server, network and Internet infrastructure. 
I was involved in the deployment of all our new foreign customers and
have actively designed all the procedure of remote management and support.

2008-2010 System Engineer
-------------------------
Advensys S.A at Brussels Belgium.
Identical job to the previous one but mostly in charge of the backup 
and user support, but also everything that was out of the ordinary.  


Education
=========

1988-1991 Université catholique de Louvain-la-neuve
---------------------------------------------------
To get a degree in math (4 year) aborted in 2nd.
Anyway I sill have good mathematical knowledges that give
me big advantage in day to day work as developer 
and system engineer.    

1991-1995 Diploma in Computer science (3 years)
-----------------------------------------------
Institut Paul Lambin, Bruxelles
 

Foreign languages
=================

* French:  mother language.
* English: fluent.
* German: school knowledges, and some practial use  
* Duch: school knowledges.

Misc
====

* Born in 1970
* Belgian.
* Have a girlfriend.
* I like sport and nature.

  